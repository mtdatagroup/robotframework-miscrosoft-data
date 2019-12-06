FROM centos:7 AS BASE

LABEL description="Data-X Test Framework"
LABEL maintainer="Mark Stella, Takuya Truong"
LABEL version="1.0.0"

RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo
#RUN yum remove -y unixODBC-utf16 unixODBC-utf16-devel
RUN ACCEPT_EULA=Y yum -y install msodbcsql17 mssql-tools unixODBC-devel
RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm && \
    yum -y update && \
    yum -y install yum-utils wget && \
    yum -y install gcc-c++ python36u python36u-pip python36u-devel

FROM BASE AS TF

WORKDIR /usr/src/app
VOLUME /usr/src/external

ENV APP_ROOT /usr/src/app
ENV LIB_DIR /usr/src/app/lib

#ENV PATH $PATH:/opt/mssql-tools/bin

COPY requirements.txt /usr/src/app
RUN pip3.6 install -r requirements.txt

ENV PYTHONPATH ${PYTHONPATH}:${LIB_DIR}

COPY config /usr/src/app/config
COPY lib /usr/src/app/lib
COPY .env /usr/src/app
COPY bootstrap.py /usr/src/app

ENTRYPOINT ["/usr/bin/python3.6", "/usr/src/app/bootstrap.py"]

CMD []