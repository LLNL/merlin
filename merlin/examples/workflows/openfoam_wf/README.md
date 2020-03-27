# TODO
1. specify concurrency for each worker
2. split up the spec for the tutorial
# running-simulation
Pre-reqs:
1. Docker
```bash
  docker pull cfdengine/openfoam
  docker pull redis
```
2. Virtual-env
```
  Ofpp==0.11
  merlin==1.4.1
  scikit-learn==0.21.3
  matplotlib==3.1.1
```

Steps:
1. Set up a redis server using docker
```bash
  docker run --detach --name my-redis -p 6379:6379 redis
```
2. Clone this repo into work directory
3. Run the YAML script
```bash
  cd running-simulation
  merlin run openfoam-study.yaml
  merlin run openfoam-study.yaml
```
