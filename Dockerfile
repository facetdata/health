# mvn clean install

################################################################################
# Wagon image
# Used for deploying to environments
################################################################################

FROM openjdk:8 as wagon

WORKDIR /apps

COPY contrib/wagon/target/health-agent-wagon-*-selfcontained.jar /apps/lib/
COPY entrypoint.sh /

ENTRYPOINT ["sh", "/entrypoint.sh"]

################################################################################
# HQ image
# Used for deploying to environments
################################################################################

FROM openjdk:8 as hq

WORKDIR /apps

COPY hq/target/health-hq-*-selfcontained.jar /apps/lib/
COPY entrypoint.sh /

ENTRYPOINT ["sh", "/entrypoint.sh"]
