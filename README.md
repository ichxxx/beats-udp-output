# beats-udp-output
## How To Use
1. Clone this project to `elastic/beats/libbeat/output/`

2. Modify `elastic/beats/libbeat/publisher/includes/includes.go` :
   ```go
   // add import
   import _ "github.com/elastic/beats/libbeat/output/beats-udp-output"
   ```

3. Compile beats

## Configuration
### Example
```yaml
output.udp:
  host: 127.0.0.1
  port: 8080
  codec: ...
```

### Options
#### codec
Output codec configuration. If the codec section is missing, events will be json encoded using the pretty option.

See [Change the output codec](https://www.elastic.co/guide/en/beats/filebeat/master/configuration-output-codec.html) for more information.
