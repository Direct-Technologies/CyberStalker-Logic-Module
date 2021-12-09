VERSION=$(shell cat VERSION)
IMAGE=pix-dispatcher
REGISTRY=pixelcore.azurecr.io

docker:
	docker build . -t ${IMAGE}:latest -t ${IMAGE}:${VERSION} -t ${REGISTRY}/${IMAGE}:latest -t ${REGISTRY}/${IMAGE}:${VERSION}

# Deploy images to the registry
deploy:
	docker push ${REGISTRY}/${IMAGE}:latest
	docker push ${REGISTRY}/${IMAGE}:${VERSION}

# Update local repository
pull:
	git pull

# Bump version
bump: pull
	docker run --rm -v "${CURDIR}":/app treeder/bump patch

taggit:
	cat VERSION
	echo $(VERSION)
	git add VERSION 
	git commit -m "version $(VERSION)"
	git tag -a "$(VERSION)" -m "version $(VERSION)"
	git push 
	git push origin $(VERSION) --tags

# Trigger CI/CD procedure
version: bump taggit

tag:
	cat VERSION
	echo $(VERSION)
	git add VERSION
	git commit -m "version $(VERSION)"
	git tag -a $(VERSION) -m "version $(VERSION)"
	
push-develop:
	git push origin develop --tags


