import 'package:app_links/app_links.dart';

import 'bridge/api.dart';
import 'bridge_helpers.dart';

/// Handles `altersend://join/<code>` and `com.altersend.mobile://join/<code>`.
class DeepLinkService {
  DeepLinkService._();

  static final AppLinks _appLinks = AppLinks();
  static bool _started = false;

  static const _allowedSchemes = ['altersend', 'com.altersend.mobile'];

  static Future<void> start({
    required Future<void> Function(String code) onJoinCode,
  }) async {
    if (_started) return;
    _started = true;

    Future<void> handleUri(Uri? uri) async {
      if (uri == null) return;
      if (!_allowedSchemes.contains(uri.scheme)) return;

      final code = await extractJoinCode(text: uri.toString());
      if (code == null) return;
      if (!await canJoinFromDeepLink(code: code)) return;
      await onJoinCode(code);
    }

    final initial = await _appLinks.getInitialLink();
    await handleUri(initial);

    _appLinks.uriLinkStream.listen(handleUri);
  }
}
