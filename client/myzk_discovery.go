// +build myzk

package client

import (
	"github.com/docker/libkv/store/zookeeper"
)

func init() {
	zookeeper.Register()
}

