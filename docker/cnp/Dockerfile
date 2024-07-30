FROM hub.jdcloud.com/cnp/centos-cnp:centos7.9.2009-olap

ARG CH_ROOT=/etc/clickhouse-server
RUN mkdir -p ${CH_ROOT}/bin
COPY clickhouse ${CH_ROOT}/bin/
COPY scripts/* /usr/local/bin/
RUN chmod a+x /usr/local/bin/*

ENTRYPOINT ["agent", "-cmd", "config,start"]
