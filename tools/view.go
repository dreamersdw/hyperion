package main

import (
	"fmt"

	"github.com/sdming/goh"
)

func main() {

	address := "127.0.0.1:9090"

	client, err := goh.NewTcpClient(address, goh.TBinaryProtocol, false)
	if err != nil {
		fmt.Println(err)
		return
	}

	if err = client.Open(); err != nil {
		fmt.Println(err)
		return
	}

	defer client.Close()

	tableNames, err := client.GetTableNames()
	if err != nil {
		fmt.Println(err)
	}

	scan := &goh.TScan{
	//FilterString: "KeyOnlyFilter ()",
	}

	for _, tableName := range tableNames {
		fmt.Printf("%s\n", tableName)
		columnFamilyNames, _ := client.GetColumnDescriptors(tableName)
		for cf, _ := range columnFamilyNames {
			fmt.Printf("\tfamily: %s\n", cf)
			scanId, err := client.ScannerOpenWithScan(tableName, scan, map[string]string{})
			if err != nil {
				fmt.Println(err)
			}
			// data, err := client.ScannerGet(scanId)
			data, err := client.ScannerGetList(scanId, 1000000)
			if err != nil {
				fmt.Println(err)
			}
			defer client.ScannerClose(scanId)

			for _, result := range data {
				for k, v := range result.Columns {
					fmt.Printf("\t\trow: %q column: %q  value: %q  time: %d\n", result.Row, k, v.Value, v.Timestamp)
				}
			}
		}
	}

}
