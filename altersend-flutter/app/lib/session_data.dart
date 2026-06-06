import 'dart:convert';

import 'bridge/api.dart';

class SessionData {
  SessionData({required this.ui, required this.session});

  final Map<String, dynamic> ui;
  final Map<String, dynamic> session;

  static Future<SessionData> load() async {
    final uiJson = await getUiSnapshotJson();
    final sessionJson = await getSessionStateJson();
    return SessionData(
      ui: jsonDecode(uiJson) as Map<String, dynamic>,
      session: jsonDecode(sessionJson) as Map<String, dynamic>,
    );
  }
}
