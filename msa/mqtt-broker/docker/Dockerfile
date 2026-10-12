#
# Copyright © 2016-2026 The Thingsboard Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# The Debian package is only a build artifact; no Debian installer or service manager
# is part of the runtime image. Keep the existing Maven package/buildx workflow.
FROM debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f AS unpack
COPY ${pkg.name}.deb /tmp/application.deb
RUN dpkg-deb --extract /tmp/application.deb /out

FROM ${docker.base.image}

# Retain the existing numeric identity for mounted configuration and data volumes.
RUN dnf install -y --setopt=install_weak_deps=False shadow-utils \
    && dnf clean all \
    && groupadd --system --gid 799 ${pkg.user} \
    && useradd --system --no-create-home --uid 799 --gid ${pkg.user} --home-dir ${pkg.installFolder} ${pkg.user}

COPY --from=unpack --chown=${pkg.user}:${pkg.user} /out${pkg.installFolder}/ ${pkg.installFolder}/
COPY start-tb-mqtt-broker.sh /usr/bin/

RUN mkdir -p /data /var/log/${pkg.name} /etc/${pkg.name} \
    && ln -s ${pkg.installFolder}/conf /etc/${pkg.name}/conf \
    && chown ${pkg.user}:${pkg.user} /data /var/log/${pkg.name} \
    && chmod 555 ${pkg.installFolder}/bin/${pkg.name}.jar /usr/bin/start-tb-mqtt-broker.sh

USER ${pkg.user}

VOLUME ["/data"]

CMD ["start-tb-mqtt-broker.sh"]
