# Test framework

##### Setup

1. Install docker
2. (Optional) Install WSL (Ubuntu)

```
./run_me.sh [ -b | -c | -i | -p | -t | -o ]

 -b = build docker image
 -c = build docker image - use no-cache
 -i = interactive bash shell - mount local volume
 -i = interactive bash shell - no volume mounting
 -p = docker system prune
 -t = run the container
```

##### Using

Ensure you have all your features files in external/features