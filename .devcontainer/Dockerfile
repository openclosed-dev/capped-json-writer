FROM eclipse-temurin:21-jdk-noble

ARG USERNAME
ARG USER_UID
ARG USER_GID=$USER_UID

RUN userdel -r ubuntu \
  && groupadd -g ${USER_GID} ${USERNAME} \
  && useradd -u ${USER_UID} -g ${USER_GID} -m -s /bin/bash ${USERNAME}

RUN apt-get update \
  && export DEBIAN_FRONTEND=noninteractive \
  && apt-get install -y --no-install-recommends git openssh-client unzip

ARG GRADLE_VERSION

RUN wget -nv "https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip" \
  && mkdir /opt/gradle \
  && unzip -d /opt/gradle gradle-${GRADLE_VERSION}-bin.zip

ENV PATH=$PATH:/opt/gradle/gradle-${GRADLE_VERSION}/bin

RUN mkdir /workspaces && chown ${USERNAME}:${USERNAME} /workspaces

USER ${USERNAME}

RUN mkdir -p /home/${USERNAME}/.gradle/caches
