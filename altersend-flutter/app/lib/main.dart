import 'dart:async';
import 'dart:io' show Platform;

import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_rust_bridge/flutter_rust_bridge.dart' show Uint64List;
import 'package:path_provider/path_provider.dart';
import 'package:qr_flutter/qr_flutter.dart';

import 'package:image_picker/image_picker.dart';

import 'bridge/api.dart';
import 'bridge/frb_generated.dart';
import 'deep_link_service.dart';
import 'onboarding_screen.dart';
import 'onboarding_storage.dart';
import 'photos_copy_effect.dart';
import 'qr_scan_screen.dart';
import 'session_data.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await AlterSendBridge.init();
  final dir = await getApplicationSupportDirectory();
  await initEngine(storagePath: dir.path);
  runApp(const AlterSendApp());
}

class AlterSendApp extends StatelessWidget {
  const AlterSendApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'AlterSend',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF2D6AE3)),
        useMaterial3: true,
      ),
      home: const AppGate(),
    );
  }
}

class AppGate extends StatefulWidget {
  const AppGate({super.key});

  @override
  State<AppGate> createState() => _AppGateState();
}

class _AppGateState extends State<AppGate> {
  bool? _onboardingDone;

  @override
  void initState() {
    super.initState();
    isOnboardingCompleted().then((done) {
      if (mounted) setState(() => _onboardingDone = done);
    });
  }

  @override
  Widget build(BuildContext context) {
    if (_onboardingDone == null) {
      return const Scaffold(
        body: Center(child: CircularProgressIndicator()),
      );
    }
    if (_onboardingDone == false) {
      return OnboardingScreen(
        onFinish: () => setState(() => _onboardingDone = true),
      );
    }
    return const HomeShell();
  }
}

class HomeShell extends StatefulWidget {
  const HomeShell({super.key});

  @override
  State<HomeShell> createState() => _HomeShellState();
}

class _HomeShellState extends State<HomeShell> {
  int _tab = 0;
  Timer? _poll;
  final _receiveKey = GlobalKey<_ReceivePageState>();

  @override
  void initState() {
    super.initState();
    _poll = Timer.periodic(const Duration(seconds: 1), (_) {
      if (mounted) setState(() {});
    });
    DeepLinkService.start(onJoinCode: _handleDeepLinkJoin);
    PhotosCopyEffect.instance.start();
  }

  Future<void> _handleDeepLinkJoin(String code) async {
    if (!mounted) return;
    setState(() => _tab = 1);
    await _receiveKey.currentState?.joinWithCode(code);
  }

  @override
  void dispose() {
    _poll?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('AlterSend'),
        actions: [
          TextButton(
            onPressed: () async {
              await clearSession();
              if (mounted) setState(() {});
            },
            child: const Text('Reset'),
          ),
        ],
      ),
      body: IndexedStack(
        index: _tab,
        children: [
          const SendPage(),
          ReceivePage(key: _receiveKey),
        ],
      ),
      bottomNavigationBar: NavigationBar(
        selectedIndex: _tab,
        onDestinationSelected: (i) => setState(() => _tab = i),
        destinations: const [
          NavigationDestination(icon: Icon(Icons.upload_file), label: 'Send'),
          NavigationDestination(icon: Icon(Icons.download), label: 'Receive'),
        ],
      ),
    );
  }
}

class SendPage extends StatefulWidget {
  const SendPage({super.key});

  @override
  State<SendPage> createState() => _SendPageState();
}

class _SendPageState extends State<SendPage> {
  bool _busy = false;

  bool get _isMobile =>
      !kIsWeb && (Platform.isAndroid || Platform.isIOS);

  Future<void> _addPicked({
    required List<String> paths,
    required List<String> names,
    required List<int> sizes,
  }) async {
    if (paths.isEmpty) return;
    setState(() => _busy = true);
    try {
      await addSelectedFiles(
        paths: paths,
        names: names,
        sizes: Uint64List.fromList(sizes),
      );
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _pickFiles() async {
    final result = await FilePicker.pickFiles(allowMultiple: true);
    if (result == null) return;
    final paths = <String>[];
    final names = <String>[];
    final sizes = <int>[];
    for (final f in result.files) {
      if (f.path == null) continue;
      paths.add(f.path!);
      names.add(f.name);
      sizes.add(f.size);
    }
    await _addPicked(paths: paths, names: names, sizes: sizes);
  }

  Future<void> _pickPhotos() async {
    final picker = ImagePicker();
    final result = await picker.pickMultipleMedia();
    if (result.isEmpty) return;
    final paths = <String>[];
    final names = <String>[];
    final sizes = <int>[];
    for (final asset in result) {
      final path = asset.path;
      if (path.isEmpty) continue;
      paths.add(path);
      names.add(asset.name.isNotEmpty ? asset.name : path.split('/').last);
      sizes.add(await asset.length());
    }
    await _addPicked(paths: paths, names: names, sizes: sizes);
  }

  Future<void> _browse() async {
    if (!_isMobile) {
      await _pickFiles();
      return;
    }
    await showModalBottomSheet<void>(
      context: context,
      builder: (ctx) => SafeArea(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            ListTile(
              leading: const Icon(Icons.photo_library_outlined),
              title: const Text('Photos'),
              onTap: () {
                Navigator.pop(ctx);
                _pickPhotos();
              },
            ),
            ListTile(
              leading: const Icon(Icons.folder_open),
              title: const Text('Files'),
              onTap: () {
                Navigator.pop(ctx);
                _pickFiles();
              },
            ),
          ],
        ),
      ),
    );
  }

  Future<void> _share() async {
    setState(() => _busy = true);
    try {
      await continueShare();
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('$e')),
        );
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<SessionData>(
      future: SessionData.load(),
      builder: (context, snap) {
        if (!snap.hasData) {
          return const Center(child: CircularProgressIndicator());
        }
        final ui = snap.data!.ui;
        final session = snap.data!.session;
        final sendCopy = ui['send_copy'] as Map<String, dynamic>?;
        final topic = ui['topic'] as String? ?? '';
        final joinUrl = ui['join_url'] as String? ?? '';
        final selected = session['selected_files'] as List<dynamic>? ?? [];
        final uploadItems = session['upload_items'] as List<dynamic>? ?? [];
        final peers = session['connected_peers'] as Map<String, dynamic>? ?? {};
        final error = ui['error_message'] as String?;

        return ListView(
          padding: const EdgeInsets.all(24),
          children: [
            Text(
              sendCopy?['title'] ?? 'Send files',
              style: Theme.of(context).textTheme.headlineSmall,
            ),
            const SizedBox(height: 8),
            Text(sendCopy?['description'] ?? ''),
            const SizedBox(height: 24),
            FilledButton.icon(
              onPressed: _busy ? null : _browse,
              icon: Icon(_isMobile ? Icons.add : Icons.folder_open),
              label: Text(
                selected.isEmpty
                    ? (_isMobile ? 'Add files' : 'Choose files')
                    : '${selected.length} file(s) selected',
              ),
            ),
            if (selected.isNotEmpty) ...[
              const SizedBox(height: 12),
              ...selected.map((f) {
                final m = f as Map<String, dynamic>;
                return ListTile(
                  dense: true,
                  title: Text(m['name'] as String? ?? ''),
                  subtitle: Text(m['path'] as String? ?? ''),
                  trailing: IconButton(
                    icon: const Icon(Icons.close),
                    onPressed: _busy
                        ? null
                        : () async {
                            await removeSelectedFile(path: m['path'] as String);
                            if (mounted) setState(() {});
                          },
                  ),
                );
              }),
            ],
            const SizedBox(height: 12),
            FilledButton(
              onPressed: _busy || selected.isEmpty ? null : _share,
              child: _busy
                  ? const SizedBox(
                      height: 20,
                      width: 20,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Text('Send'),
            ),
            if (uploadItems.isNotEmpty) ...[
              const SizedBox(height: 24),
              Text('Preparing', style: Theme.of(context).textTheme.titleMedium),
              ...uploadItems.map((item) {
                final m = item as Map<String, dynamic>;
                return ListTile(
                  dense: true,
                  title: Text(m['name'] as String? ?? ''),
                  trailing: Text(m['status'] as String? ?? ''),
                );
              }),
            ],
            if (topic.isNotEmpty) ...[
              const SizedBox(height: 32),
              Text('Share code', style: Theme.of(context).textTheme.titleMedium),
              const SizedBox(height: 16),
              Center(
                child: Container(
                  padding: const EdgeInsets.all(16),
                  decoration: BoxDecoration(
                    color: Colors.white,
                    borderRadius: BorderRadius.circular(12),
                    border: Border.all(color: Theme.of(context).dividerColor),
                  ),
                  child: QrImageView(
                    data: joinUrl,
                    version: QrVersions.auto,
                    size: 200,
                    backgroundColor: Colors.white,
                  ),
                ),
              ),
              const SizedBox(height: 16),
              SelectableText(
                topic,
                style: const TextStyle(fontFamily: 'monospace', fontSize: 13),
              ),
              const SizedBox(height: 8),
              Row(
                children: [
                  Expanded(
                    child: OutlinedButton.icon(
                      onPressed: () {
                        Clipboard.setData(ClipboardData(text: topic));
                        ScaffoldMessenger.of(context).showSnackBar(
                          const SnackBar(content: Text('Code copied')),
                        );
                      },
                      icon: const Icon(Icons.copy),
                      label: const Text('Copy code'),
                    ),
                  ),
                  const SizedBox(width: 8),
                  Expanded(
                    child: OutlinedButton.icon(
                      onPressed: () {
                        Clipboard.setData(ClipboardData(text: joinUrl));
                        ScaffoldMessenger.of(context).showSnackBar(
                          const SnackBar(content: Text('Link copied')),
                        );
                      },
                      icon: const Icon(Icons.link),
                      label: const Text('Copy link'),
                    ),
                  ),
                ],
              ),
              if (ui['is_peer_connected'] == true) ...[
                const SizedBox(height: 16),
                Text(
                  '${peers.length} peer(s) connected',
                  style: TextStyle(color: Theme.of(context).colorScheme.primary),
                ),
              ],
              const SizedBox(height: 8),
              Text(
                'Closing the app cancels the transfer.',
                style: Theme.of(context).textTheme.bodySmall,
              ),
            ],
            if (error != null && error.isNotEmpty) ...[
              const SizedBox(height: 16),
              Text(
                error,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              ),
            ],
          ],
        );
      },
    );
  }
}

class ReceivePage extends StatefulWidget {
  const ReceivePage({super.key});

  @override
  State<ReceivePage> createState() => _ReceivePageState();
}

class _ReceivePageState extends State<ReceivePage> {
  final _codeController = TextEditingController();
  bool _busy = false;

  bool get _isMobile =>
      !kIsWeb && (Platform.isAndroid || Platform.isIOS);

  Future<void> joinWithCode(String code) async {
    if (!await isValidJoinCode(code: code)) return;
    setState(() => _busy = true);
    try {
      await joinSession(joinCode: code);
      _codeController.text = code;
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('$e')),
        );
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _join() async {
    final raw = _codeController.text.trim();
    final extracted = await extractJoinCode(text: raw);
    final code = extracted ?? raw;
    if (!await isValidJoinCode(code: code)) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Enter a valid 64-character code')),
        );
      }
      return;
    }
    await joinWithCode(code);
  }

  Future<void> _scanQr() async {
    final code = await Navigator.of(context).push<String>(
      MaterialPageRoute(builder: (_) => const QrScanScreen()),
    );
    if (code != null) {
      await joinWithCode(code);
    }
  }

  Future<void> _downloadAll() async {
    setState(() => _busy = true);
    try {
      await downloadAllFiles();
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('$e')),
        );
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  @override
  void dispose() {
    _codeController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<SessionData>(
      future: SessionData.load(),
      builder: (context, snap) {
        if (!snap.hasData) {
          return const Center(child: CircularProgressIndicator());
        }
        final ui = snap.data!.ui;
        final session = snap.data!.session;
        final receiveCopy = ui['receive_copy'] as Map<String, dynamic>?;
        final step = ui['receive_step'] as String? ?? 'join';
        final offers = session['incoming_file_offers'] as List<dynamic>? ?? [];
        final downloads =
            session['receive_download_states'] as Map<String, dynamic>? ?? {};
        final error = ui['error_message'] as String?;
        final showJoin = step == 'join' || step == 'connecting';

        return ListView(
          padding: const EdgeInsets.all(24),
          children: [
            Text(
              receiveCopy?['title'] ?? 'Receive files',
              style: Theme.of(context).textTheme.headlineSmall,
            ),
            const SizedBox(height: 8),
            Text(receiveCopy?['description'] ?? ''),
            const SizedBox(height: 24),
            if (showJoin) ...[
              TextField(
                controller: _codeController,
                decoration: const InputDecoration(
                  labelText: 'Join code',
                  hintText: '64-character hex code',
                  border: OutlineInputBorder(),
                ),
                maxLines: 2,
              ),
              const SizedBox(height: 12),
              FilledButton(
                onPressed: _busy ? null : _join,
                child: const Text('Connect'),
              ),
              if (_isMobile) ...[
                const SizedBox(height: 12),
                OutlinedButton.icon(
                  onPressed: _busy ? null : _scanQr,
                  icon: const Icon(Icons.qr_code_scanner),
                  label: const Text('Scan QR code'),
                ),
              ],
            ],
            if (offers.isNotEmpty) ...[
              const SizedBox(height: 24),
              Text(
                'Incoming files',
                style: Theme.of(context).textTheme.titleMedium,
              ),
              ...offers.map((o) {
                final m = o as Map<String, dynamic>;
                final id = m['id'] as String;
                final st = downloads[id] as Map<String, dynamic>?;
                final status = st?['status'] as String? ?? 'idle';
                final total = (st?['total_bytes'] as num?)?.toInt() ?? m['size'];
                final done = (st?['bytes_transferred'] as num?)?.toInt() ?? 0;
                final pct = total > 0 ? ((done / total) * 100).round() : 0;
                final destination = st?['destination'] as String?;
                final savedLabel = status == 'completed' && destination == 'photos'
                    ? 'Saved to Photos'
                    : status == 'completed'
                        ? 'Saved'
                        : null;
                return ListTile(
                  title: Text(m['name'] as String? ?? ''),
                  subtitle: Text(
                    savedLabel ??
                        (status == 'downloading'
                            ? '$pct% · ${_formatBytes(done)} / ${_formatBytes(total)}'
                            : _formatBytes(total)),
                  ),
                  trailing: status == 'completed'
                      ? const Icon(Icons.check_circle, color: Colors.green)
                      : null,
                );
              }),
              const SizedBox(height: 12),
              if (step == 'incoming_transfer')
                FilledButton(
                  onPressed: _busy ? null : _downloadAll,
                  child: const Text('Download all'),
                ),
            ],
            if (error != null && error.isNotEmpty) ...[
              const SizedBox(height: 16),
              Text(
                error,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              ),
            ],
          ],
        );
      },
    );
  }

  String _formatBytes(int bytes) {
    if (bytes < 1024) return '$bytes B';
    if (bytes < 1024 * 1024) return '${(bytes / 1024).toStringAsFixed(1)} KB';
    if (bytes < 1024 * 1024 * 1024) {
      return '${(bytes / (1024 * 1024)).toStringAsFixed(1)} MB';
    }
    return '${(bytes / (1024 * 1024 * 1024)).toStringAsFixed(1)} GB';
  }
}
