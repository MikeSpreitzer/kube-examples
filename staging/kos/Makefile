DOCKER_PREFIX=${LOGNAME}

publish:: publish-network-apiserver
publish:: publish-ipam-controller
publish:: publish-connection-agent

build:: build-network-apiserver
build:: build-ipam-controller
build:: build-connection-agent
build:: build-attachment-tput-driver

clean:
	rm -f images/*/*r
	rm -f images/*/*/*[rt]
	rm -rf local-binaries

build-network-apiserver:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o images/network-apiserver/network-apiserver k8s.io/examples/staging/kos/cmd/network-apiserver

build-ipam-controller:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o images/controllers/ipam/ipam-controller k8s.io/examples/staging/kos/cmd/controllers/ipam

build-connection-agent:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o images/controllers/connectionagent/connection-agent k8s.io/examples/staging/kos/cmd/controllers/connectionagent

build-attachment-tput-driver:
	go build -a -o local-binaries/attachment-tput-driver k8s.io/examples/staging/kos/cmd/attachment-tput-driver

publish-network-apiserver:
	m4 -DDOCKER_PREFIX=${DOCKER_PREFIX} deploy.m4/50-rs-xs.yaml.m4 > deploy/50-rs-xs.yaml
	cd images/network-apiserver && docker build -t ${DOCKER_PREFIX}/kos-network-apiserver:latest . && docker push ${DOCKER_PREFIX}/kos-network-apiserver:latest

publish-ipam-controller:
	m4 -DDOCKER_PREFIX=${DOCKER_PREFIX} deploy.m4/50-rs-ipam.yaml.m4 > deploy/50-rs-ipam.yaml
	cd images/controllers/ipam && docker build -t ${DOCKER_PREFIX}/kos-ipam-controller:latest . && docker push ${DOCKER_PREFIX}/kos-ipam-controller:latest

publish-connection-agent:
	m4 -DDOCKER_PREFIX=${DOCKER_PREFIX} deploy.m4/50-ds-ca.yaml.m4 > deploy/50-ds-ca.yaml
	cp -R cmd/attachment-tput-driver/test-scripts images/controllers/connectionagent
	cd images/controllers/connectionagent && docker build -t ${DOCKER_PREFIX}/kos-connection-agent:latest . && docker push ${DOCKER_PREFIX}/kos-connection-agent:latest

.PHONY: deploy
deploy:
	kubectl apply -f deploy

.PHONY: undeploy
undeploy:
	kubectl delete -f deploy
