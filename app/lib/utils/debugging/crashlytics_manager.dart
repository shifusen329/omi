// File named crashlytics_manager.dart for backward compat with `PlatformManager`
// references. The implementation routes through Sentry now — Firebase Crashlytics
// has been removed.
import 'package:flutter/material.dart';

import 'package:sentry_flutter/sentry_flutter.dart';

import 'package:omi/utils/debugging/crash_reporter.dart';
import 'package:omi/utils/platform/platform_service.dart';

class CrashlyticsManager implements CrashReporter {
  static final CrashlyticsManager _instance = CrashlyticsManager._internal();
  static CrashlyticsManager get instance => _instance;

  CrashlyticsManager._internal();

  factory CrashlyticsManager() {
    return _instance;
  }

  /// Kept as a no-op so existing `await CrashlyticsManager.init()` callers
  /// don't have to change. Sentry init happens in `main.dart`.
  static Future<void> init() async {}

  @override
  void identifyUser(String email, String name, String userId) {
    PlatformService.executeIfSupported(true, () {
      Sentry.configureScope((scope) {
        scope.setUser(SentryUser(
          id: userId.isEmpty ? null : userId,
          email: email.isEmpty ? null : email,
          username: name.isEmpty ? null : name,
        ));
      });
    });
  }

  @override
  void logInfo(String message) {
    PlatformService.executeIfSupported(
      true,
      () => Sentry.addBreadcrumb(Breadcrumb(message: message, level: SentryLevel.info)),
    );
  }

  @override
  void logError(String message) {
    PlatformService.executeIfSupported(
      true,
      () => Sentry.addBreadcrumb(Breadcrumb(message: message, level: SentryLevel.error)),
    );
  }

  @override
  void logWarn(String message) {
    PlatformService.executeIfSupported(
      true,
      () => Sentry.addBreadcrumb(Breadcrumb(message: message, level: SentryLevel.warning)),
    );
  }

  @override
  void logDebug(String message) {
    PlatformService.executeIfSupported(
      true,
      () => Sentry.addBreadcrumb(Breadcrumb(message: message, level: SentryLevel.debug)),
    );
  }

  @override
  void logVerbose(String message) {
    PlatformService.executeIfSupported(
      true,
      () => Sentry.addBreadcrumb(Breadcrumb(message: message, level: SentryLevel.debug)),
    );
  }

  @override
  void setUserAttribute(String key, String value) {
    PlatformService.executeIfSupported(
      true,
      () => Sentry.configureScope((scope) => scope.setTag(key, value)),
    );
  }

  /// Sentry has no global on/off switch analogous to Crashlytics collection.
  /// We keep the signature but treat it as a no-op; disabling is done by not
  /// calling SentryFlutter.init (i.e., leaving SENTRY_DSN empty in env).
  @override
  void setEnabled(bool isEnabled) {}

  @override
  Future<void> reportCrash(Object exception, StackTrace stackTrace, {Map<String, String>? userAttributes}) async {
    await PlatformService.executeIfSupportedAsync(true, () async {
      await Sentry.captureException(
        exception,
        stackTrace: stackTrace,
        withScope: (scope) {
          if (userAttributes != null) {
            for (final entry in userAttributes.entries) {
              scope.setTag(entry.key, entry.value);
            }
          }
        },
      );
    });
  }

  @override
  NavigatorObserver? getNavigatorObserver() {
    // SentryNavigatorObserver captures route changes as breadcrumbs and
    // performance transactions. Install it in the app's navigator observers
    // list (see MyApp MaterialApp / Navigator configuration).
    return SentryNavigatorObserver();
  }

  @override
  bool get isSupported => true;
}
