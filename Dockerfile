From alpine:3.7 AS build-env
RUN apk add --no-cache --update python py2-pip python-dev libc-dev gcc
RUN pip install happybase tornado redis hbase-thrift thrift


FROM alpine:3.7
RUN apk add --update --no-cache python 
WORKDIR /hbase_store
COPY ./ /hbase_store
COPY --from=build-env /usr/lib/python2.7/site-packages /usr/lib/python2.7/site-packages
EXPOSE 28888
ENTRYPOINT python service.py -g -e dev -p 28888
