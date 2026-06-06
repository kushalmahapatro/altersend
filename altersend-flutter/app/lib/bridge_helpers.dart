import 'dart:convert';

import 'bridge/api.dart';

/// Matches `altersend_domain::build_join_url`.
String buildJoinUrl({required String topic}) {
  return 'com.altersend.mobile://join/$topic';
}

/// Matches `altersend_domain::can_join_from_deep_link` using session JSON from Rust.
Future<bool> canJoinFromDeepLink({required String code}) async {
  final session =
      jsonDecode(await getSessionStateJson()) as Map<String, dynamic>;
  final role = session['role'];
  if (role == 'sender') return false;
  final topic = session['topic'] as String? ?? '';
  if (topic.isNotEmpty && topic != code) return false;
  return true;
}
