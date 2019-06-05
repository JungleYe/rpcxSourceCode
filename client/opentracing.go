package client

import (
	"context"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/smallnest/rpcx/share"
)

type OpenTracingPlugin struct{}

//插件-在rpc发起调用之前执行该方法；
func (p *OpenTracingPlugin) DoPreCall(ctx context.Context, servicePath, serviceMethod string, args interface{}) error {
	var span1 opentracing.Span

	// if it is called in rpc service in case that a service calls antoher service,
	// we uses the span in the service context as the parent span.
	parentSpan := ctx.Value(share.OpentracingSpanServerKey)
	if parentSpan != nil {
		//如果父span存在，就建立父span与新span的父子关系；
		span1 = opentracing.StartSpan(
			"rpcx.client."+servicePath+"."+serviceMethod,
			opentracing.ChildOf(parentSpan.(opentracing.Span).Context()))
	} else {
		//创建span
		wireContext, err := share.GetSpanContextFromContext(ctx)
		if err == nil && wireContext != nil { //try to parse span from request
			span1 = opentracing.StartSpan(
				"rpcx.client."+servicePath+"."+serviceMethod,
				ext.RPCServerOption(wireContext))
		} else { // parse span from context or create root context
			span1, _ = opentracing.StartSpanFromContext(ctx, "rpcx.client."+servicePath+"."+serviceMethod)
		}
	}

	//把span放到ctx中，方便跨方法传递
	if rpcxContext, ok := ctx.(*share.Context); ok {
		rpcxContext.SetValue(share.OpentracingSpanClientKey, span1)
	}
	return nil
}

//插件-在rpc调用之后执行该方法；
func (p *OpenTracingPlugin) DoPostCall(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}, err error) error {
	//从context中取出span，然后结束该span，并上报链路追踪后台；
	if rpcxContext, ok := ctx.(*share.Context); ok {
		span1 := rpcxContext.Value(share.OpentracingSpanClientKey)
		if span1 != nil {
			span1.(opentracing.Span).Finish()
		}
	}
	return nil
}
