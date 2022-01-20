# gothrift

Golang thrift Server and Client

一个简单的thrift服务与客户端封装

传输层使用：`BufferedTransport` 二进制传输

协议层使用：`BinaryProtocol` 二进制协议

处理器使用：`MultiplexedProcessor` 多服务处理器

## 示例

[examples](https://github.com/wenjy/gothrift/tree/main/examples)

### 创建测试目录

`mkdir thrift-go-server`

`mkdir thrift-go-client`

### 生成`thrift`文件

Test1.thrift
```
namespace go test1

/**
* 测试
**/
service Test1 {
    string hello1();
    string hello2();
}
```

Test2.thrift
```
namespace go test2

/**
* 测试
**/
service Test2 {
    string hello1();
    string hello2();
}
```

- 使用命令生成

`thrift-0.14.2.exe --gen go Test1.thrift`

`thrift-0.14.2.exe --gen go Test2.thrift`

- 把文件夹复制到项目

`cp test1 thrift-go-server/`

`cp test2 thrift-go-server/`

`cp test1 thrift-go-client/`

`cp test2 thrift-go-client/`

- 删除里面的remote示例代码 `test1-remote` `test2-remote.go`

### 服务端代码

`go mod init thriftgos`

`go get github.com/wenjy/gothrift`

main.go

```golang
package main

import (
	"context"
	"fmt"
	"thriftgos/test1"
	"thriftgos/test2"

	"github.com/wenjy/gothrift/server"
)

func main() {
	addr := "localhost:9090"
	handler1 := newTest1Handler()
	handler2 := newTest2Handler()

	tServer := server.NewTServer(addr)
	tServer.RegisterProcessor("test1", test1.NewTest1Processor(handler1))
	tServer.RegisterProcessor("test2", test2.NewTest2Processor(handler2))
	err := tServer.Serve()
	if err != nil {
		fmt.Println(err)
	}
}

type test1Handler struct {
}

func newTest1Handler() *test1Handler {
	return &test1Handler{}
}

func (h *test1Handler) Hello1(ctx context.Context) (r string, err error) {
	return "test1->hello1", nil
}

func (h *test1Handler) Hello2(ctx context.Context) (r string, err error) {
	return "test1->hello2", nil
}

type test2Handler struct {
}

func newTest2Handler() *test2Handler {
	return &test2Handler{}
}

func (h *test2Handler) Hello1(ctx context.Context) (r string, err error) {
	return "test2->hello1", nil
}

func (h *test2Handler) Hello2(ctx context.Context) (r string, err error) {
	return "test2->hello2", nil
}

```


### 客户端代码

`go mod init thriftgoc`

`go get github.com/wenjy/gothrift`

main.go

```golang
package main

import (
	"context"
	"fmt"
	"thriftgoc/test1"
	"thriftgoc/test2"
	"time"

	"github.com/wenjy/gothrift/client"
)

func main() {
	addr := "localhost:9090"

	mc := client.NewMClient(addr, time.Second, time.Second)

	c1, err := mc.Client("test1")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c1.Trans.Close()
	client1 := test1.NewTest1Client(c1.Client)
	fmt.Println(client1.Hello1(context.Background()))
	fmt.Println(client1.Hello2(context.Background()))

	c2, err := mc.Client("test2")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c2.Trans.Close()
	client2 := test2.NewTest2Client(c2.Client)
	fmt.Println(client2.Hello1(context.Background()))
	fmt.Println(client2.Hello2(context.Background()))
}

```

### 构建&运行

服务端：

`go build`

`./thriftgos`

客户端：

`go build`

`./thriftgoc`

输出：
```
test1->hello1 <nil>
test1->hello2 <nil>
test2->hello1 <nil>
test2->hello2 <nil>
```
