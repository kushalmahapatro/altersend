import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';

import 'bridge/api.dart';
import 'download_routing.dart';

/// Watches session state and routes completed downloads to Photos on mobile.
class PhotosCopyEffect {
  PhotosCopyEffect._();

  static final PhotosCopyEffect instance = PhotosCopyEffect._();

  Timer? _timer;
  final Set<String> _processed = {};

  void start() {
    _timer ??= Timer.periodic(const Duration(milliseconds: 500), (_) {
      unawaited(_evaluate());
    });
  }

  void stop() {
    _timer?.cancel();
    _timer = null;
    _processed.clear();
  }

  Future<void> _evaluate() async {
    try {
      final session =
          jsonDecode(await getSessionStateJson()) as Map<String, dynamic>;
      final offers =
          session['incoming_file_offers'] as List<dynamic>? ?? [];
      final downloads =
          session['receive_download_states'] as Map<String, dynamic>? ?? {};

      if (downloads.isEmpty) {
        _processed.clear();
        return;
      }

      for (final entry in downloads.entries) {
        final offerKey = entry.key;
        final item = entry.value as Map<String, dynamic>;
        if (item['status'] != 'completed') continue;
        if (item['destination'] != null) continue;
        if (_processed.contains(offerKey)) continue;

        final savedTo = item['saved_to'] as String?;
        if (savedTo == null || savedTo.isEmpty) continue;

        final offer = offers.cast<Map<String, dynamic>?>().firstWhere(
              (o) => o?['id'] == offerKey,
              orElse: () => null,
            );
        if (offer == null) continue;

        _processed.add(offerKey);
        final fileName = offer['name'] as String? ?? '';
        final routing = await handleDownloadedFile(savedTo, fileName);
        await notifyDownloadRouted(offerKey: offerKey, routing: routing);
      }
    } catch (e, st) {
      debugPrint('PhotosCopyEffect: $e\n$st');
    }
  }
}
