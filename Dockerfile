FROM ubuntu

# General Stuff
ENV TZ=Europe/Minsk
ENV DEBIAN_FRONTEND=noninteractive 
RUN apt-get update
RUN apt-get install -y g++ build-essential make cmake unzip zip git curl tar wget pkg-config nano

# Get Boost, openssl, cpprestsdk
RUN apt-get install -y libboost-all-dev libssl-dev libcpprest-dev

# Download VCPKG in / and install dependencies
WORKDIR /
RUN git clone https://github.com/microsoft/vcpkg
WORKDIR /vcpkg
RUN ./bootstrap-vcpkg.sh
RUN ./vcpkg integrate install
RUN ./vcpkg install nlohmann-json:x64-linux

# Copy Source Files to /usr/src/
WORKDIR /usr/src/
COPY . .

# Build and Compile /usr/src to /usr/build/
RUN mkdir /usr/build/
WORKDIR /usr/build/
RUN cmake /usr/src/ -DCMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-linux
RUN make

# Create dataFolder
RUN mkdir /home/databaseData

EXPOSE 3399
VOLUME /home/databaseData

ENTRYPOINT ["./amableDB", "--dataPath", "/home/databaseData/", "--apiAddress", "0.0.0.0"]