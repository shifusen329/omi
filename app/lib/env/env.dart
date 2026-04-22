abstract class Env {
  static late final EnvFields _instance;
  static String? _apiBaseUrlOverride;
  static String? _agentProxyWsUrlOverride;
  static bool isTestFlight = false;

  static void init(EnvFields instance) {
    _instance = instance;
  }

  static void overrideApiBaseUrl(String url) {
    _apiBaseUrlOverride = url;
  }

  static void overrideAgentProxyWsUrl(String url) {
    _agentProxyWsUrlOverride = url;
  }

  static String? get openAIAPIKey => _instance.openAIAPIKey;

  static String? get mixpanelProjectToken => _instance.mixpanelProjectToken;

  // static String? get apiBaseUrl => 'https://omi-backend.ngrok.app/';
  static String? get apiBaseUrl => _apiBaseUrlOverride ?? _instance.apiBaseUrl;

  /// Staging API URL from STAGING_API_URL env var. Null when not configured.
  static String? get stagingApiUrl {
    final url = _instance.stagingApiUrl;
    if (url == null || url.isEmpty) return null;
    return url;
  }

  /// Whether STAGING_API_URL is configured in the environment.
  static bool get isStagingConfigured => stagingApiUrl != null;

  static bool get isUsingStagingApi {
    final effective = apiBaseUrl;
    final staging = stagingApiUrl;
    if (effective == null || staging == null) return false;
    return _normalizeUrl(effective) == _normalizeUrl(staging);
  }

  static String _normalizeUrl(String url) {
    var s = url.trim().toLowerCase();
    while (s.endsWith('/')) {
      s = s.substring(0, s.length - 1);
    }
    return s;
  }

  /// WebSocket URL for the agent proxy service.
  /// Derives from apiBaseUrl: api.omi.me → agent.omi.me, api.omiapi.com → agent.omiapi.com.
  /// Can be overridden via Env.overrideAgentProxyWsUrl() for local testing.
  static String get agentProxyWsUrl {
    if (_agentProxyWsUrlOverride != null) return _agentProxyWsUrlOverride!;
    final base = apiBaseUrl ?? 'https://api.omi.me';
    final host = Uri.parse(base).host.replaceFirst('api.', 'agent.');
    return 'wss://$host/v1/agent/ws';
  }

  static String? get growthbookApiKey => _instance.growthbookApiKey;

  static String? get googleMapsApiKey => _instance.googleMapsApiKey;

  static String? get intercomAppId => _instance.intercomAppId;

  static String? get intercomIOSApiKey => _instance.intercomIOSApiKey;

  static String? get intercomAndroidApiKey => _instance.intercomAndroidApiKey;

  static String? get googleClientId => _instance.googleClientId;

  static String? get googleClientSecret => _instance.googleClientSecret;

  static bool get useWebAuth => _instance.useWebAuth ?? false;

  static bool get useAuthCustomToken => _instance.useAuthCustomToken ?? false;

  // ----- Self-host OIDC (active when authProvider == 'oidc') -----

  /// 'firebase' (default) or 'oidc'. When 'oidc', the app authenticates
  /// against the configured OIDC issuer instead of Firebase Auth.
  static String get authProvider => (_instance.authProvider ?? 'firebase').toLowerCase();

  /// True when the OIDC self-host auth path is selected.
  static bool get isOidcAuth => authProvider == 'oidc';

  static String? get oidcIssuer => _instance.oidcIssuer;

  static String? get oidcClientId => _instance.oidcClientId;

  /// Discovery URL — defaults to {issuer}/.well-known/openid-configuration.
  static String? get oidcDiscoveryUrl {
    final explicit = _instance.oidcDiscoveryUrl;
    if (explicit != null && explicit.isNotEmpty) return explicit;
    final issuer = oidcIssuer;
    if (issuer == null || issuer.isEmpty) return null;
    final trimmed = issuer.endsWith('/') ? issuer.substring(0, issuer.length - 1) : issuer;
    return '$trimmed/.well-known/openid-configuration';
  }

  /// Custom-scheme deep link the AppAuth flow redirects back to.
  /// Reuses the existing com.friend.ios scheme that's already wired up
  /// for Apple OAuth callbacks.
  static String get oidcRedirectUri =>
      _instance.oidcRedirectUri ?? 'com.friend.ios://oauth/callback';

  static List<String> get oidcScopes =>
      (_instance.oidcScopes ?? 'openid email profile')
          .split(RegExp(r'\s+'))
          .where((s) => s.isNotEmpty)
          .toList();

  // ----- Sentry -----

  /// DSN for the mobile Sentry project. Leave unset to disable Sentry entirely
  /// (useful for dev builds).
  static String? get sentryDsn {
    final v = _instance.sentryDsn;
    if (v == null || v.isEmpty) return null;
    return v;
  }

  static double get sentryTracesSampleRate {
    final v = _instance.sentryTracesSampleRate;
    if (v == null || v.isEmpty) return 0.1;
    return double.tryParse(v) ?? 0.1;
  }

  static String get sentryEnv => _instance.sentryEnv ?? 'selfhost';
}

abstract class EnvFields {
  String? get openAIAPIKey;

  String? get mixpanelProjectToken;

  String? get apiBaseUrl;

  String? get growthbookApiKey;

  String? get googleMapsApiKey;

  String? get intercomAppId;

  String? get intercomIOSApiKey;

  String? get intercomAndroidApiKey;

  String? get googleClientId;

  String? get googleClientSecret;

  bool? get useWebAuth;

  bool? get useAuthCustomToken;

  String? get stagingApiUrl;

  // OIDC self-host auth (optional; null when authProvider != 'oidc').
  String? get authProvider;

  String? get oidcIssuer;

  String? get oidcClientId;

  String? get oidcDiscoveryUrl;

  String? get oidcRedirectUri;

  String? get oidcScopes;

  // Sentry (optional; null disables)
  String? get sentryDsn;

  String? get sentryTracesSampleRate;

  String? get sentryEnv;
}
