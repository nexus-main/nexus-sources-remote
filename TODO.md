# TODO

## Reverse readData Arrow multiplexing

The top-level remote `read` data socket now uses Arrow IPC directly. The reverse
`readData` callback path still uses the existing request-id frame envelope and
places a self-contained Arrow IPC stream inside successful data frames.

That design is correct for concurrent `readData` calls because responses can be
correlated before their Arrow payloads are fully reassembled, but it is still a
custom protocol layer around Arrow.

A cleaner design would make the reverse `readData` response socket an Arrow IPC
stream as well and move correlation into the Arrow schema, for example:

```text
requestId: int32
kind: int8          # data, end, error
offset: int64
values: list<float64>
error: utf8
```

This would remove the custom binary frame wrapper while still allowing multiple
concurrent `readData` responses to be demultiplexed by request id.
