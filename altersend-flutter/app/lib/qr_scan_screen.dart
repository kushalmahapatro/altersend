import 'package:flutter/material.dart';
import 'package:mobile_scanner/mobile_scanner.dart';

import 'bridge/api.dart';

class QrScanScreen extends StatefulWidget {
  const QrScanScreen({super.key});

  @override
  State<QrScanScreen> createState() => _QrScanScreenState();
}

class _QrScanScreenState extends State<QrScanScreen> {
  bool _handled = false;

  Future<void> _onDetect(BarcodeCapture capture) async {
    if (_handled) return;
    final raw = capture.barcodes.firstOrNull?.rawValue;
    if (raw == null || raw.isEmpty) return;

    final code = await extractJoinCode(text: raw);
    if (code == null) return;
    if (!await isValidJoinCode(code: code)) return;

    _handled = true;
    if (mounted) {
      Navigator.of(context).pop(code);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Scan join code')),
      body: MobileScanner(
        onDetect: _onDetect,
      ),
    );
  }
}
