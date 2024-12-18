package lib

import (
	"fmt"
	"time"
)

func TimeNowToString() string {
	now := time.Now()
	return fmt.Sprintf("%d-%s-%d %d_%d_%d", now.Year(), now.Month().String(), now.Day(), now.Hour(), now.Minute(), now.Second())
}
