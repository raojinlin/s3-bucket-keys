package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"strings"
	"sync"
	"time"
)

var client *s3.Client

func getClient() *s3.Client {
	if client != nil {
		return client
	}
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client = s3.NewFromConfig(cfg)
	return client
}

func getBucketKeysAndSize(bucket, prefix string, callback func(page int64, object types.Object)) (int64, int64) {
	client := getClient()
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	var startTime = time.Now()
	var totalSize, totalKeys, page int64
	for {
		output, err := client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			log.Fatal(err)
		}

		if len(output.Contents) == 0 {
			break
		}

		for i, object := range output.Contents {
			totalKeys++
			totalSize += object.Size
			if len(output.Contents)-1 == i {
				input.StartAfter = object.Key

				page++
				fmt.Printf("\rs3://%s/%s, keys=%d, size=%s, page=%d, time=%s",
					bucket,
					prefix,
					totalKeys,
					bytesToReadable(float64(totalSize)),
					page,
					time.Duration(time.Now().UnixNano()-startTime.UnixNano()).String(),
				)
			}

			if callback != nil {
				callback(page, object)
			}
		}
	}

	fmt.Println()
	return totalKeys, totalSize
}

func bytesToReadable(bytes float64) string {
	if bytes <= 1024 {
		return fmt.Sprintf("%.2fB", bytes)
	}

	kb := bytes / 1024
	if kb <= 1024 {
		return fmt.Sprintf("%.2fK", kb)
	}

	mb := kb / 1024
	if mb <= 1024 {
		return fmt.Sprintf("%.2fM", mb)
	}

	gb := mb / 1024
	if gb < 1024 {
		return fmt.Sprintf("%.2fG", gb)
	}

	tb := gb / 1024
	return fmt.Sprintf("%.2fT", tb)
}

func diff(bucket1, bucket2, prefix string) {
	wg := sync.WaitGroup{}
	wg.Add(2)

	var bucket1Keys, bucket1Size int64
	var bucket2Keys, bucket2Size int64

	go func() {
		defer wg.Done()
		bucket1Keys, bucket1Size = getBucketKeysAndSize(bucket1, prefix, nil)
	}()

	go func() {
		defer wg.Done()
		bucket2Keys, bucket2Size = getBucketKeysAndSize(bucket2, prefix, nil)
	}()

	wg.Wait()

	if bucket2Keys < bucket1Keys {
		fmt.Printf("bucket2 %s keys(%d) size(%s) less than bucket1 %s keys(%d) size(%s)\n", bucket2, bucket2Keys,
			bytesToReadable(float64(bucket2Size)), bucket1, bucket1Keys, bytesToReadable(float64(bucket1Size)))
	}
}

func main() {
	var bucket, prefix string
	var isDiff = false
	flag.StringVar(&bucket, "bucket", "", "S3 bucket name, Up to two buckets are supported, and multiple buckets are separated by commas")
	flag.StringVar(&prefix, "prefix", "", "S3 object key prefix, eg. /path/to/some-key")
	flag.BoolVar(&isDiff, "diff", false, "Compare whether the key prefix is consistent in the two buckets (by the number and size of keys)")
	flag.Parse()

	if bucket == "" || prefix == "" {
		flag.Usage()
		return
	}

	buckets := strings.Split(bucket, ",")
	if isDiff {
		diff(buckets[0], buckets[1], prefix)
		return
	}

	getBucketKeysAndSize(buckets[0], prefix, nil)

}
