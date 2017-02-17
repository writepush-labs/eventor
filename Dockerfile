FROM alpine:3.5

ARG EVENTOR_VERSION
ARG EVENTOR_SHA

RUN set -ex && apk add --no-cache ca-certificates

ENV GOLANG_VERSION 1.7.5
ENV GOLANG_SRC_URL https://golang.org/dl/go$GOLANG_VERSION.src.tar.gz
ENV GOLANG_SRC_SHA256 4e834513a2079f8cbbd357502cccaac9507fd00a1efe672375798858ff291815
ENV EVENTOR_SRC_URL https://github.com/writepush-labs/eventor/archive/$EVENTOR_VERSION.tar.gz
ENV EVENTOR_BUILD_PATH /go/src/github.com/writepush-labs

# https://golang.org/issue/14851
ADD https://raw.githubusercontent.com/docker-library/golang/master/1.7/alpine/no-pic.patch /
# https://golang.org/issue/17847
ADD https://raw.githubusercontent.com/docker-library/golang/master/1.7/alpine/17847.patch /

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV CGO_ENABLED 1

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

RUN set -ex \
	&& apk add --no-cache --virtual .build-deps \
		bash \
		gcc \
		musl-dev \
		openssl \
		go \
		git \
		glide \
	\
	&& export GOROOT_BOOTSTRAP="$(go env GOROOT)" \
	&& wget -q "$GOLANG_SRC_URL" -O golang.tar.gz \
	&& echo "$GOLANG_SRC_SHA256  golang.tar.gz" | sha256sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz \
	&& cd /usr/local/go/src \
	&& patch -p2 -i /no-pic.patch \
	&& patch -p2 -i /17847.patch \
	&& ./make.bash \
	&& rm -rf /*.patch \
	&& mkdir -p $EVENTOR_BUILD_PATH && cd $EVENTOR_BUILD_PATH \
    && wget -q "$EVENTOR_SRC_URL" -O eventor.tar.gz \
    && tar -xzf eventor.tar.gz \
    && mv "eventor-$EVENTOR_VERSION" eventor \
    && rm eventor.tar.gz \
    && cd eventor \
    && glide install \
    && go build -v -ldflags "-X main.VERSION=$EVENTOR_VERSION" \
    && mkdir /writepush-labs && mv eventor /writepush-labs/eventor && chmod +x /writepush-labs/eventor \
    && mkdir /writepush-labs/data \
    && rm -rf $GOPATH && rm -rf /usr/local/go && rm -rf /root/.glide \
    && apk del .build-deps gcc musl-dev go git glide bash

WORKDIR /writepush-labs

ENTRYPOINT ["./eventor"]

EXPOSE 9400