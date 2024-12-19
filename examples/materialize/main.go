package main

import (
	"fmt"
	"log"

	dagsterpipes "github.com/hupe1980/dagster-pipes-go"
)

func main() {
	fmt.Println("Start!")

	session, err := dagsterpipes.New[map[string]any]()
	if err != nil {
		log.Fatalf("Error creating dagster pipes session: %v", err)
	}
	defer session.Close()

	if err := session.Run(func(context *dagsterpipes.Context[map[string]any]) error {
		if err := context.ReportAssetMaterialization(&dagsterpipes.AssetMaterialization{
			AssetKey:    "materialize_subprocess",
			DataVersion: "1.0",
			Metadata: map[string]any{
				"foo": "bar",
			},
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Error running dagster pipes session: %v", err)
	}

	fmt.Println("End!")
}
