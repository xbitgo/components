package database

import "context"

var saasDBMapKey = struct{}{}

func saasDBMap(ctx context.Context) map[string]interface{} {
	if v := ctx.Value(saasDBMapKey); v != nil {
		switch vm := v.(type) {
		case map[string]interface{}:
			return vm
		}
	}
	return nil
}

func SetSaasDBMap(ctx context.Context, dbMap map[string]interface{}) context.Context {
	return context.WithValue(ctx, saasDBMapKey, dbMap)
}
