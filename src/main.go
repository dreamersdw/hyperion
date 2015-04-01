package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/docopt/docopt-go"
	"github.com/sdming/goh"
	"github.com/sdming/goh/Hbase"
)

var (
	thriftServer = "127.0.0.1:9090"
)

type HourSpan struct {
	hour       int64
	tags       map[string]string
	datapoints []DataPoint
}

type Task struct {
	start  int64
	end    int64
	metric string
	client *goh.HClient
}

type DataPoint struct {
	timestamp int64
	value     float64
}

type Record struct {
	mid   Text
	time  uint32
	rtags Text
}

type DataPoints []DataPoint
type RowResult *Hbase.TRowResult
type Text []byte

func (dps DataPoints) Avg() float64 {
	if len(dps) == 0 {
		return 0.0
	}
	sum := 0.0
	for _, dp := range dps {
		sum += dp.value
	}
	return sum / float64(len(dps))
}

func (dps DataPoints) Max() float64 {
	if len(dps) == 0 {
		return 0.0
	}

	max := dps[0].value
	for _, dp := range dps {
		if max < dp.value {
			max = dp.value
		}
	}
	return max
}

func (dps DataPoints) Min() float64 {
	if len(dps) == 0 {
		return 0.0
	}

	min := dps[0].value
	for _, dp := range dps {
		if min > dp.value {
			min = dp.value
		}
	}
	return min
}

func rowToDataPoints(result RowResult, client *goh.HClient) (r Record, alldps DataPoints, err error) {
	record, err := parseRow([]byte(result.Row))
	checkError(err)

	for column, cell := range result.Columns {
		dps, err := parseDataPoint(record.time, column, cell.Value)
		alldps = append(alldps, dps...)
		checkError(err)
	}
	return record, alldps, nil
}

func NewTask(thriftServer, metric string, day string) *Task {
	theDay, err := time.Parse("2006-01-02", day)
	checkError(err)

	client, err := goh.NewTcpClient(thriftServer, goh.TBinaryProtocol, false)
	checkError(err)
	checkError(client.Open())

	return &Task{
		start:  theDay.Unix(),
		end:    theDay.Unix() + 24*3600,
		metric: metric,
		client: client,
	}
}

func (t *Task) Save() {
}

func (t *Task) Collect() map[string]DataPoints {
	client := t.client
	metric := t.metric
	start, end := t.start, t.end

	metricID, err := queryMetricID([]byte(metric), client)
	checkError(err)

	startBytes, endBytes := make([]byte, 4), make([]byte, 4)
	binary.BigEndian.PutUint32(startBytes, uint32(start))
	binary.BigEndian.PutUint32(endBytes, uint32(end))

	tableName, scan := "tsdb", &goh.TScan{
		StartRow: append(metricID, startBytes...),
		StopRow:  append(metricID, endBytes...),
		Columns:  []string{"t"},
	}

	scanId, err := client.ScannerOpenWithScan(tableName, scan, map[string]string{})
	checkError(err)

	data, err := client.ScannerGetList(scanId, 10*1000*1000)
	checkError(err)

	defer client.ScannerClose(scanId)

	timeSeries := make(map[string]DataPoints, 1500*10000)

	for _, rowResult := range data {
		record, dps, err := rowToDataPoints(rowResult, client)
		checkError(err)

		key := string(append(record.mid, record.rtags...))
		if v, ok := timeSeries[key]; ok {
			timeSeries[key] = append(v, dps...)
		} else {
			timeSeries[key] = dps
		}
	}

	return timeSeries
}

type AggResult struct {
	avg float64
	max float64
	min float64
}

type MetricAggResult struct {
	metric string
	tags   map[string]string
	avg    float64
	max    float64
	min    float64
}

func (mr MetricAggResult) String() string {
	return fmt.Sprintf("metric=%s, tags=%s avg=%f max=%f min=%f",
		mr.metric, mr.tags, mr.avg, mr.max, mr.min)
}

func (t *Task) Execute() (mr []MetricAggResult) {
	ts := t.Collect()
	result := make(map[string]AggResult)

	for key, value := range ts {
		result[key] = AggResult{
			avg: value.Avg(),
			max: value.Max(),
			min: value.Min(),
		}
	}

	for key, value := range result {
		bs := []byte(key)
		mid := bs[:3]
		metric, err := queryMetric(mid, t.client)
		checkError(err)

		rtags := bs[3:]
		tags := map[string]string{}
		for i := 0; i < len(bs)-6; i += 6 {
			tagkID := rtags[i : i+3]
			tagvID := rtags[i+3 : i+6]
			tagk, err := queryTagk(tagkID, t.client)
			checkError(err)
			tagv, err := queryTagv(tagvID, t.client)
			checkError(err)
			tags[string(tagk)] = string(tagv)
		}

		mr = append(mr, MetricAggResult{
			metric: string(metric),
			avg:    value.avg,
			max:    value.max,
			min:    value.min,
			tags:   tags,
		})
	}
	return
}

func parseRow(row Text) (Record, error) {
	if len(row) < 13 || (len(row)-7)%6 != 0 {
		return Record{}, errors.New("")
	}

	rtags := row[7:]

	return Record{
		mid:   row[:3],
		time:  uint32(row[3])<<24 | uint32(row[4])<<16 | uint32(row[5])<<8 | uint32(row[6]),
		rtags: rtags,
	}, nil
}

func makeQuery(table string, column string) func([]byte, *goh.HClient) ([]byte, error) {
	metaCache := make(map[string]string, 1000000)
	return func(row []byte, client *goh.HClient) ([]byte, error) {
		key := table + string(row) + column

		if v, ok := metaCache[key]; ok {
			return []byte(v), nil
		}

		cell, err := client.Get(table, row, column, nil)
		if err != nil {
			return []byte{}, err
		}

		if len(cell) != 1 {
			return []byte{}, fmt.Errorf("when query %q, hbase return %d result", row, len(cell))
		}

		metaCache[key] = string(cell[0].Value)
		return cell[0].Value, nil
	}
}

var (
	queryMetric = makeQuery("tsdb-uid", "name:metrics")
	queryTagk   = makeQuery("tsdb-uid", "name:tagk")
	queryTagv   = makeQuery("tsdb-uid", "name:tagv")

	queryMetricID = makeQuery("tsdb-uid", "id:metrics")
	queryTagkID   = makeQuery("tsdb-uid", "id:tagk")
	queryTagvID   = makeQuery("tsdb-uid", "id:tagv")
)

func parseInt(data []byte, length uint) uint64 {
	var result uint64
	switch length {
	case 1:
		result = uint64(data[0])
	case 2:
		result = uint64(binary.BigEndian.Uint16(data))
	case 4:
		result = uint64(binary.BigEndian.Uint32(data))
	case 8:
		result = uint64(binary.BigEndian.Uint64(data))
	}
	return result
}

func parseFloat(data []byte, length uint) float64 {
	var result float64
	switch length {
	case 4:
		result = float64(math.Float32frombits(binary.BigEndian.Uint32(data)))
	case 8:
		result = math.Float64frombits(binary.BigEndian.Uint64(data))
	}
	return result
}

func parseDataPoint(hourTime uint32, column string, rawValue []byte) (dps []DataPoint, err error) {
	bytes := []byte(column)[2:]

	if len(bytes) < 2 || len(bytes)%2 != 0 {
		err = errors.New("column must be at least 2 bytes")
		return
	}

	if len(bytes) == 2 {
		dp := DataPoint{}
		dp.timestamp = int64(hourTime + uint32(bytes[0])<<4 | uint32(bytes[1])>>4)

		typeFlag := (uint32(bytes[1]) >> 3) & 1
		lenFlag := uint32(bytes[1]) & (0xf >> 1)

		var dataLen uint
		switch lenFlag {
		case 0:
			dataLen = 1
		case 2:
			dataLen = 2
		case 3:
			dataLen = 4
		case 4:
			dataLen = 8
		default:
			// TODO: we should raise an error here
			dataLen = 4
		}

		if typeFlag == 0 {
			dp.value = float64(parseInt(rawValue, dataLen))
		} else {
			dp.value = parseFloat(rawValue, dataLen)
		}
		dps = []DataPoint{dp}
		return
	}

	var offset uint = 0

	for i := 0; i <= len(bytes)-2; i += 2 {
		dp := DataPoint{}
		bs := bytes[i : i+2]
		dp.timestamp = int64(hourTime + uint32(bs[0])<<4 | uint32(bs[1])>>4)

		typeFlag := (uint32(bs[1]) >> 3) & 1
		lenFlag := uint32(bs[1]) & (0xf >> 1)

		var dataLen uint
		switch lenFlag {
		case 0:
			dataLen = 1
		case 2:
			dataLen = 2
		case 3:
			dataLen = 4
		case 4:
			dataLen = 8
		default:
			// TODO: we should raise an error here
			dataLen = 4
		}

		rv := rawValue[offset : offset+dataLen]

		if typeFlag == 0 {
			dp.value = float64(parseInt(rv, dataLen))
		} else {
			dp.value = parseFloat(rv, dataLen)
		}

		dps = append(dps, dp)
	}

	return

}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
}

const (
	version = "0.1.0"
	usage   = `Usage:
	hyperion query [--thrift=ADDR] [METRIC] [DAY]
	hyperion server [--thrift=ADDR] [--port=PORT]
	hyperion version
	hyperion help

Example:
    hypersion query sys.cpu.usage 2015-02-03
	`
)

func handleQuery(w http.ResponseWriter, r *http.Request) {
	metric := r.FormValue("metric")
	day := r.FormValue("day")
	t := NewTask(thriftServer, metric, day)
	mr := t.Execute()
	for _, m := range mr {
		fmt.Println(m.String())
	}

	bytes, err := json.Marshal(mr)
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(bytes)
}

func main() {
	opt, err := docopt.Parse(usage, nil, false, "", false, false)
	checkError(err)

	if opt["version"].(bool) {
		fmt.Println(version)
		os.Exit(0)
	}

	if opt["help"].(bool) {
		fmt.Println(usage)
		os.Exit(0)
	}

	var (
		thriftServer = "127.0.0.1:9090"
		metric       = "sys.cpu.usage"
		day          = "2015-03-24"
	)

	if opt["query"].(bool) {
		if opt["--thrift"] != nil {
			thriftServer = opt["--thrift"].(string)
		}

		if opt["METRIC"] != nil {
			metric = opt["METRIC"].(string)
		}

		if opt["DAY"] != nil {
			day = opt["DAY"].(string)
		}

		t := NewTask(thriftServer, metric, day)
		mr := t.Execute()
		for _, m := range mr {
			fmt.Println(m.String())
		}
		return
	}

	if opt["server"].(bool) {
		var port int64 = 42421
		if opt["--port"] != nil {
			strPort := opt["--port"].(string)
			port, err = strconv.ParseInt(strPort, 10, 32)
			checkError(err)
		}
		address := fmt.Sprintf("0.0.0.0:%d", port)
		http.HandleFunc("/api/metric/daily", handleQuery)
		http.ListenAndServe(address, nil)
	}
}
