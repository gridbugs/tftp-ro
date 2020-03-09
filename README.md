# Read-Only TFTP Server

Server implementing a subset of TFTP allowing it to respond to read requests only.
The intended use case is when you have a directory of (say) binary images you wish to upload to (say) a dev board
running (say) u-boot.

## Example

This will run a server in the foreground, on the specified host and port, serving files out of the specified directory.

```
$ sudo tftp-ro --address=0.0.0.0:69 --directory=images
```
