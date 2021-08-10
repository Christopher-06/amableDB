FROM alpine

# General Stuff
ENV TZ=Europe/Minsk
ENV DEBIAN_FRONTEND=noninteractive 
RUN apk update
RUN apk add alpine-sdk curl zip unzip tar make cmake git pkgconfig nano

# Get Boost, openssl, nhlohmann-json
RUN apk add boost-dev openssl nlohmann-json

# Settings
RUN mkdir /usr/build/ \
		&& mkdir /home/databaseData

EXPOSE 3399
VOLUME /home/databaseData

# Copy Source Files to /usr/src/
WORKDIR /usr/src/
COPY . .

# Build and Compile /usr/src to /usr/build/
WORKDIR /usr/build/
RUN cmake /usr/src/ && make 

ENTRYPOINT ["./amableDB", "--dataPath", "/home/databaseData/", "--apiAddress", "0.0.0.0"]