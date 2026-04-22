import 'dart:convert';
import 'dart:math';

import 'package:flutter_appauth/flutter_appauth.dart';
import 'package:flutter_secure_storage/flutter_secure_storage.dart';

import 'package:omi/backend/preferences.dart';
import 'package:omi/env/env.dart';
import 'package:omi/utils/logger.dart';

/// OIDC sign-in / token storage / refresh — used when [Env.isOidcAuth] is true.
///
/// Lives behind [AuthService] (Firebase remains the default). All token state
/// is kept in [FlutterSecureStorage] so it survives app restarts. The id_token
/// is also mirrored to [SharedPreferencesUtil.authToken] for the existing HTTP
/// header injection path in shared.dart, which doesn't know about OIDC.
class OidcAuthService {
  static final OidcAuthService instance = OidcAuthService._();
  OidcAuthService._();

  final _appAuth = const FlutterAppAuth();
  static const _storage = FlutterSecureStorage();

  // Storage keys
  static const _kIdToken = 'oidc_id_token';
  static const _kAccessToken = 'oidc_access_token';
  static const _kRefreshToken = 'oidc_refresh_token';
  static const _kExpiresAtMs = 'oidc_expires_at_ms';
  static const _kSub = 'oidc_sub';

  // Refresh threshold — refresh when token has < 5 min left.
  static const _refreshLeewayMs = 5 * 60 * 1000;

  /// True if a stored id_token exists. Doesn't check expiry; callers should
  /// rely on [getIdToken] to handle refresh.
  Future<bool> hasStoredToken() async {
    return (await _storage.read(key: _kIdToken)) != null;
  }

  /// Synchronous-friendly check used by [AuthService.isSignedIn]: relies on
  /// [SharedPreferencesUtil.authToken] which we mirror on every successful
  /// sign-in / refresh.
  bool isSignedInSync() {
    return SharedPreferencesUtil().authToken.isNotEmpty;
  }

  /// Returns the cached id_token, refreshing via the refresh_token if it's
  /// expired or about to expire. Returns null if no session exists or refresh
  /// fails.
  Future<String?> getIdToken({bool forceRefresh = false}) async {
    final stored = await _storage.read(key: _kIdToken);
    final expiresAtMs = int.tryParse(await _storage.read(key: _kExpiresAtMs) ?? '') ?? 0;
    final now = DateTime.now().millisecondsSinceEpoch;
    final stillValid = stored != null && expiresAtMs - now > _refreshLeewayMs;
    if (stillValid && !forceRefresh) {
      return stored;
    }
    return _refresh();
  }

  Future<String?> _refresh() async {
    final refreshToken = await _storage.read(key: _kRefreshToken);
    if (refreshToken == null) {
      Logger.debug('OIDC refresh skipped: no refresh_token stored');
      return null;
    }
    final discoveryUrl = Env.oidcDiscoveryUrl;
    if (discoveryUrl == null || discoveryUrl.isEmpty) {
      Logger.debug('OIDC refresh skipped: OIDC_DISCOVERY_URL not configured');
      return null;
    }
    final clientId = Env.oidcClientId;
    if (clientId == null || clientId.isEmpty) {
      Logger.debug('OIDC refresh skipped: OIDC_CLIENT_ID not configured');
      return null;
    }
    try {
      final result = await _appAuth.token(
        TokenRequest(
          clientId,
          Env.oidcRedirectUri,
          discoveryUrl: discoveryUrl,
          refreshToken: refreshToken,
          grantType: 'refresh_token',
          scopes: Env.oidcScopes,
        ),
      );
      await _persist(result);
      return result.idToken;
    } catch (e) {
      Logger.debug('OIDC refresh failed: $e');
      return null;
    }
  }

  /// Launch the AppAuth flow (Chrome Custom Tab on Android / ASWebAuthSession
  /// on iOS), exchange the resulting code, store tokens, and mirror the
  /// id_token + uid into SharedPreferences for the rest of the app.
  Future<bool> signInWithBrowser({String? loginHintProvider}) async {
    final discoveryUrl = Env.oidcDiscoveryUrl;
    if (discoveryUrl == null || discoveryUrl.isEmpty) {
      Logger.debug('OIDC sign-in skipped: OIDC_DISCOVERY_URL not configured');
      return false;
    }
    final clientId = Env.oidcClientId;
    if (clientId == null || clientId.isEmpty) {
      Logger.debug('OIDC sign-in skipped: OIDC_CLIENT_ID not configured');
      return false;
    }
    try {
      final additional = <String, String>{};
      if (loginHintProvider != null) {
        // The auth service supports provider hints via query params on the
        // authorize URL. Harmless if the issuer ignores it.
        additional['provider_hint'] = loginHintProvider;
      }
      final result = await _appAuth.authorizeAndExchangeCode(
        AuthorizationTokenRequest(
          clientId,
          Env.oidcRedirectUri,
          discoveryUrl: discoveryUrl,
          scopes: Env.oidcScopes,
          allowInsecureConnections: false,
          additionalParameters: additional.isEmpty ? null : additional,
        ),
      );
      await _persist(result);
      return true;
    } catch (e) {
      Logger.debug('OIDC sign-in failed: $e');
      return false;
    }
  }

  Future<void> signOut() async {
    await _storage.delete(key: _kIdToken);
    await _storage.delete(key: _kAccessToken);
    await _storage.delete(key: _kRefreshToken);
    await _storage.delete(key: _kExpiresAtMs);
    await _storage.delete(key: _kSub);
    SharedPreferencesUtil().authToken = '';
    SharedPreferencesUtil().tokenExpirationTime = 0;
  }

  Future<void> _persist(dynamic result) async {
    final idToken = result.idToken as String?;
    final accessToken = result.accessToken as String?;
    final refreshToken = result.refreshToken as String?;
    final accessTokenExp = result.accessTokenExpirationDateTime as DateTime?;

    if (idToken != null) {
      await _storage.write(key: _kIdToken, value: idToken);
    }
    if (accessToken != null) {
      await _storage.write(key: _kAccessToken, value: accessToken);
    }
    if (refreshToken != null) {
      await _storage.write(key: _kRefreshToken, value: refreshToken);
    }
    final expMs = accessTokenExp?.millisecondsSinceEpoch ?? _expFromIdToken(idToken);
    if (expMs != null) {
      await _storage.write(key: _kExpiresAtMs, value: expMs.toString());
    }

    // Mirror to SharedPreferences for the existing HTTP layer.
    if (idToken != null) {
      SharedPreferencesUtil().authToken = idToken;
      if (expMs != null) {
        SharedPreferencesUtil().tokenExpirationTime = expMs;
      }
      final claims = _decodeJwtClaims(idToken);
      if (claims != null) {
        final sub = claims['sub'];
        if (sub is String && sub.isNotEmpty) {
          await _storage.write(key: _kSub, value: sub);
          SharedPreferencesUtil().uid = sub;
        }
        final email = claims['email'];
        if (email is String && email.isNotEmpty && SharedPreferencesUtil().email.isEmpty) {
          SharedPreferencesUtil().email = email;
        }
        final name = claims['name'];
        if (name is String && name.isNotEmpty && SharedPreferencesUtil().givenName.isEmpty) {
          final parts = name.split(' ');
          SharedPreferencesUtil().givenName = parts.first;
          if (parts.length > 1) {
            SharedPreferencesUtil().familyName = parts.sublist(1).join(' ');
          }
        }
      }
    }
  }

  /// Decode the unverified payload of a JWT just to read claims locally.
  /// Signature verification happens server-side; we only use this to populate
  /// SharedPreferences with sub/email/name.
  Map<String, dynamic>? _decodeJwtClaims(String? token) {
    if (token == null) return null;
    final parts = token.split('.');
    if (parts.length != 3) return null;
    try {
      final pad = '=' * ((4 - parts[1].length % 4) % 4);
      final payload = utf8.decode(base64Url.decode(parts[1] + pad));
      final decoded = json.decode(payload);
      if (decoded is Map<String, dynamic>) return decoded;
      return null;
    } catch (_) {
      return null;
    }
  }

  int? _expFromIdToken(String? token) {
    final claims = _decodeJwtClaims(token);
    final exp = claims?['exp'];
    if (exp is int) return exp * 1000;
    if (exp is double) return (exp * 1000).round();
    return null;
  }
}

// Suppress unused import in builds without flutter_appauth platform plugins.
// ignore: unused_element
String _unused() => Random().nextInt(1).toString();
