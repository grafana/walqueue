# V2 File Format

The V2 file format uses a simple binary format based on efficiency and not on backwards compatibility or future compatibility. If there is a need for changes it should be created a separate version.

The library `benc` was chosen because it was the lowest level library that offered buffer reuse, unsafe string usage and still had reasonable checks for out of bounds, and issues. This makes it more performant that `mus` and `msgp` format for example. The source of narrowing down serialization libaries came from https://alecthomas.github.io/go_serialization_benchmarks/. 

Tested with mus,gencode,benc,bepod,mmsgp, and fastape. Benc hit the sweetspot on usage, nice api with reasonable checks.

The underlying format stores a type byte `metric` or `metadata`, specific metadata for each time and then the raw byte array of the protobuf bytes. This means it does not need to fully deserialize the data.