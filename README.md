## s3-bucket-keys
s3-bucket-keys 是一个命令行工具，可用于查询 S3 存储桶中对象数量和对象大小。它支持前缀匹配，可以查询特定前缀的对象，并实时输出统计信息以告知进度。

![](./s3-bucket-keys-example.png)

### 特性：
1. 比s3cmd更快
2. 实时输出当前统计的信息：对象大小、对象数量、耗时
3. 支持对比两个桶中相同key是否一致

### 使用：

你可以通过以下命令从源代码构建并安装 s3-bucket-keys：

```shell
$ go mod tidy
$ go build .
$ ./s3-bucket-keys -bucket YOU_BUCKET_NAME -prefix YOU_KEY_PREFIX
# 对比key在两个桶中是否不一致
$ ./s3-bucket-keys -bucket SOURCE_BUCKET,DSTINATION_BUCKET_NAME -prefix YOUR_KEY_PREFIX -diff
```

命令参数：
`s3-bucket-keys`支持以下命令参数：
```shell
  -bucket string
        S3 bucket name, Up to two buckets are supported, and multiple buckets are separated by commas
  -diff
        Compare whether the key prefix is consistent in the two buckets (by the number and size of keys)
  -prefix string
        S3 object key prefix, eg. /path/to/some-key

```

### 配置访问秘钥
在使用 s3-bucket-keys 之前，你需要配置你的AWS访问秘钥。你可以按照[Get your AWS access keys](https://aws.github.io/aws-sdk-go-v2/docs/getting-started/#get-your-aws-access-keys)中的说明进行配置。
