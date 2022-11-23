package main

import (
	"context"
	"errors"
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

const (
	MaxKeys = 1000
)

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

type Bucket struct {
	Name string `json:"name"`
	Prefix string `json:"prefix"`
	StartAfter string `json:"start_after"`
	Size int64 `json:"size"`
	Keys int64 `json:"keys"`
	StartTime time.Time `json:"start_time"`
}

func newBucket(name, prefix string) *Bucket {
	return &Bucket{
		Name:       name,
		Prefix:     prefix,
		StartAfter: "",
		Size:       0,
		Keys:       0,
		StartTime:  time.Time{},
	}
}

func (b *Bucket) getLogFile() string  {
	return fmt.Sprintf(
		"s3_%s_%s.log",
		b.Name,
		strings.Replace(strings.TrimLeft(b.Prefix, "/"), "/", "_", 1024),
	)
}

func (b *Bucket) Log()  {
	fmt.Printf("\rs2://%s/%s, keys=%d, size=%s, time=%s",
		b.Name,
		b.Prefix,
		b.Keys,
		bytesToReadable(float64(b.Size)),
		time.Duration(time.Now().UnixNano()-b.StartTime.UnixNano()).String(),
	)
}

func (b *Bucket) FetchObjects(callback func(bucket *Bucket, object types.Object)) error {
	if b.Name == "" {
		return errors.New("empty bucket name")
	}
	client := getClient()
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.Name),
		Prefix: aws.String(b.Prefix),
		MaxKeys: MaxKeys,
	}

	for {
		output, err := client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			return err
		}

		if len(output.Contents) == 0 {
			break
		}

		for i, object := range output.Contents {
			b.Keys++
			b.Size += object.Size
			if len(output.Contents)-1 == i {
				input.StartAfter = object.Key
				b.StartAfter = aws.ToString(object.Key)
				b.Log()
			}

			if callback != nil {
				callback(b, object)
			}
		}

	}
	fmt.Println()
	return nil
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
		b := newBucket(bucket1, prefix)
		b.FetchObjects(nil)
		bucket1Keys = b.Keys
		bucket1Size = b.Size
	}()

	go func() {
		defer wg.Done()
		b := newBucket(bucket2, prefix)
		b.FetchObjects(nil)
		bucket2Size = b.Size
		bucket2Keys = b.Keys
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
	var resume = false
	flag.StringVar(&bucket, "bucket", "", "S3 bucket name, Up to two buckets are supported, and multiple buckets are separated by commas")
	flag.StringVar(&prefix, "prefix", "", "S3 object key prefix, eg. /path/to/some-key, Optional. If it is empty, all objects will be queried.")
	flag.BoolVar(&isDiff, "diff", false, "Compare whether the key prefix is consistent in the two buckets (by the number and size of keys)")
	flag.BoolVar(&resume, "resume", false, "Recover from the last stop")
	flag.Parse()

	if bucket == "" {
		flag.Usage()
		return
	}

	buckets := strings.Split(bucket, ",")
	if isDiff {
		diff(buckets[0], buckets[1], prefix)
		return
	}

	b := newBucket(buckets[0], prefix)
	err := b.FetchObjects(nil)
	if err != nil {
		log.Fatal(err)
	}

}
