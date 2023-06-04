FROM hadoop-base

ADD sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz /usr/lib/sqoop


HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870

CMD ["/run.sh"]
# ENTRYPOINT ["tail", "-f", "/dev/null"]
