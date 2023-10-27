# syncr
sync plugins for [go-micro.dev/v4](https://github.com/go-micro/go-micro)

`syncr.NewMemorySync` fork from https://github.com/go-micro/plugins/tree/main/v4/sync/memory
`syncr.NewConsulSync` fork from https://github.com/go-micro/plugins/tree/main/v4/sync/consul


## wait

- `> 0`, wait for some time if failing get
- `= 0`, no wait, return err if failing get
- `< 0`, wait forever if failing get

## ttl

- `> 0`, auto unlock after ttl
- `<= 0`, never auto unlock
