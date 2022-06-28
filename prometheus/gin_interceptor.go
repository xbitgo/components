package prometheus

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GinInterceptor .
func GinInterceptor() gin.HandlerFunc {
	return func(c *gin.Context) {
		st := time.Now()
		c.Next()
		HistogramVec.Timing("http_"+MetricName(c.Request.RequestURI), map[string]string{
			"method": c.Request.Method,
			"status": strconv.Itoa(c.Writer.Status()),
		}, st)
	}
}
