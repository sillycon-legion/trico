FROM mcr.microsoft.com/azurelinux/base/core:3.0 AS build
RUN tdnf install -y cargo rust ca-certificates \
    && tdnf clean all
COPY . /build
WORKDIR /build
RUN cargo build --release

FROM mcr.microsoft.com/dotnet/runtime:9.0-azurelinux3.0-distroless-extra
COPY --from=build /build/target/release/trico /usr/bin/trico
ENTRYPOINT [ "/usr/bin/trico", "/config.toml" ]
