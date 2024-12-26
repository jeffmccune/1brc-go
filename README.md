# One Billion Row Challenge

Focused not so much on raw speed, but on readability and low cognitive load.

## Usage

```bash
# make some data
./scripts/create_measurements.py 100_000

# generate a profile
go run . -profile < measurements.txt

# build with profile-guided optimization using default.pgo
go build

# run it
./1brc-go < measurements.txt

# view profile (requires graphviz - brew install graphviz)
go tool pprof -http 127.0.0.1:8080 default.pgo
```
