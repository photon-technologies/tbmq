/**
 * Copyright © 2016-2026 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.mqtt.broker.service.security.auth.jwt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SkipPathRequestMatcherTest {
    private final SkipPathRequestMatcher matcher = new SkipPathRequestMatcher(
            List.of("/api/auth/login", "/api/auth/token", "/api/noauth/**"), "/api/**");

    @ParameterizedTest
    @ValueSource(strings = {"/api/auth/login", "/api/auth/token", "/api/noauth", "/api/noauth/activate"})
    void publicAuthenticationEndpointsSkipJwtProcessing(String path) {
        assertThat(matcher.matches(request("", path))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/mqtt/publish", "/api/auth/user", "/api/noauth-other", "/api/auth/loginExtra",
            "/api/auth/login/child", "/api/auth/login/", "/api/NOAUTH/activate"})
    void protectedPathsAndPublicLookalikesRequireJwtProcessing(String path) {
        assertThat(matcher.matches(request("", path))).isTrue();
    }

    @Test
    void contextPathAndQueryParametersDoNotBypassJwtProcessing() {
        MockHttpServletRequest request = request("/broker", "/api/mqtt/publish");
        request.setQueryString("next=/api/noauth/activate");
        assertThat(matcher.matches(request)).isTrue();
        assertThat(matcher.matches(request("/broker", "/api/auth/login"))).isFalse();
    }

    @Test
    void nonApiPathsRemainOutsideThisFilter() {
        assertThat(matcher.matches(request("", "/index.html"))).isFalse();
    }

    private static MockHttpServletRequest request(String contextPath, String path) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", contextPath + path);
        request.setContextPath(contextPath);
        request.setServletPath(path);
        return request;
    }
}
