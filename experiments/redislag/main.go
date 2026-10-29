// Command redislag writes keys on a Redis primary and reads each one back from
// a replica right away, counting how often the replica does not have it yet.
package main

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	primaryAddr := flag.String("primary", "redis-primary:6379", "primary address")
	replicaAddr := flag.String("replica", "redis-replica1:6379", "replica address")
	n := flag.Int("n", 10000, "rounds")
	writers := flag.Int("writers", 0, "connections writing 100 KB values to the primary meanwhile")
	flag.Parse()

	ctx := context.Background()
	primary := redis.NewClient(&redis.Options{Addr: *primaryAddr})
	replica := redis.NewClient(&redis.Options{Addr: *replicaAddr})
	big := string(make([]byte, 100<<10))
	for w := range *writers {
		go func() {
			for j := 0; ; j++ {
				primary.Set(ctx, "redislag:load:"+strconv.Itoa(w)+":"+strconv.Itoa(j%100), big, time.Minute)
			}
		}()
	}

	var waits []time.Duration
	for i := range *n {
		key := "redislag:" + strconv.Itoa(i)
		if err := primary.Set(ctx, key, "x", time.Minute).Err(); err != nil {
			panic(err)
		}
		start := time.Now()
		if replica.Get(ctx, key).Err() == redis.Nil {
			for replica.Get(ctx, key).Err() == redis.Nil {
			}
			waits = append(waits, time.Since(start))
		}
	}

	fmt.Printf("%d of %d reads right after the write missed on the replica\n", len(waits), *n)
	if len(waits) > 0 {
		slices.Sort(waits)
		fmt.Printf("time until visible: p50 %v, p99 %v, max %v\n",
			waits[len(waits)/2], waits[len(waits)*99/100], waits[len(waits)-1])
	}
}
