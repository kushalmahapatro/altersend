import 'dart:async';
import 'dart:convert';
import 'package:flutter_rust_bridge/flutter_rust_bridge.dart' show Uint64List;

import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:path_provider/path_provider.dart';

import 'bridge/api.dart';
import 'bridge/frb_generated.dart';

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
      home: const HomeShell(),
    );
  }
}

class HomeShell extends StatefulWidget {
  const HomeShell({super.key});

  @override
  State<HomeShell> createState() => _HomeShellState();
}

class _HomeShellState extends State<HomeShell> {
  int _tab = 0;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('AlterSend')),
      body: IndexedStack(
        index: _tab,
        children: const [SendPage(), ReceivePage()],
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
  Map<String, dynamic>? _ui;
  bool _busy = false;

  Future<void> _refresh() async {
    final json = await getUiSnapshotJson();
    setState(() => _ui = jsonDecode(json) as Map<String, dynamic>);
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
    if (paths.isEmpty) return;
    setState(() => _busy = true);
    try {
      await addSelectedFiles(paths: paths, names: names, sizes: Uint64List.fromList(sizes));
      await _refresh();
    } finally {
      setState(() => _busy = false);
    }
  }

  Future<void> _share() async {
    setState(() => _busy = true);
    try {
      await continueShare();
      await _refresh();
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text(e.toString())),
        );
      }
    } finally {
      setState(() => _busy = false);
    }
  }

  @override
  void initState() {
    super.initState();
    _refresh();
  }

  @override
  Widget build(BuildContext context) {
    final ui = _ui;
    final sendCopy = ui?['send_copy'] as Map<String, dynamic>?;
    final topic = ui?['topic'] as String? ?? '';
    final count = ui?['selected_file_count'] as int? ?? 0;

    return Padding(
      padding: const EdgeInsets.all(24),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Text(sendCopy?['title'] ?? 'Send files',
              style: Theme.of(context).textTheme.headlineSmall),
          const SizedBox(height: 8),
          Text(sendCopy?['description'] ?? ''),
          const SizedBox(height: 24),
          FilledButton.icon(
            onPressed: _busy ? null : _pickFiles,
            icon: const Icon(Icons.folder_open),
            label: Text(count == 0 ? 'Choose files' : '$count file(s) selected'),
          ),
          const SizedBox(height: 12),
          FilledButton(
            onPressed: _busy || count == 0 ? null : _share,
            child: _busy
                ? const SizedBox(
                    height: 20,
                    width: 20,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Text('Send'),
          ),
          if (topic.isNotEmpty) ...[
            const SizedBox(height: 32),
            Text('Join code', style: Theme.of(context).textTheme.titleMedium),
            const SizedBox(height: 8),
            SelectableText(topic, style: const TextStyle(fontFamily: 'monospace')),
            const SizedBox(height: 8),
            OutlinedButton.icon(
              onPressed: () {
                Clipboard.setData(ClipboardData(text: topic));
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(content: Text('Code copied')),
                );
              },
              icon: const Icon(Icons.copy),
              label: const Text('Copy code'),
            ),
          ],
          if (ui?['error_message'] != null) ...[
            const SizedBox(height: 16),
            Text(ui!['error_message'] as String,
                style: TextStyle(color: Theme.of(context).colorScheme.error)),
          ],
        ],
      ),
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
  Map<String, dynamic>? _ui;
  bool _busy = false;

  Future<void> _refresh() async {
    final json = await getUiSnapshotJson();
    setState(() => _ui = jsonDecode(json) as Map<String, dynamic>);
  }

  Future<void> _join() async {
    final raw = _codeController.text.trim();
    final extracted = await extractJoinCode(text: raw);
    final code = extracted ?? raw;
    if (!await isValidJoinCode(code: code)) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Enter a valid 64-character code')),
      );
      return;
    }
    setState(() => _busy = true);
    try {
      await joinSession(joinCode: code);
      await _refresh();
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text(e.toString())),
        );
      }
    } finally {
      setState(() => _busy = false);
    }
  }

  Future<void> _downloadAll() async {
    setState(() => _busy = true);
    try {
      await downloadAllFiles();
      await _refresh();
    } finally {
      setState(() => _busy = false);
    }
  }

  @override
  void initState() {
    super.initState();
    _refresh();
    Timer.periodic(const Duration(seconds: 2), (_) => _refresh());
  }

  @override
  void dispose() {
    _codeController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final ui = _ui;
    final receiveCopy = ui?['receive_copy'] as Map<String, dynamic>?;
    final incoming = ui?['incoming_file_count'] as int? ?? 0;
    final step = ui?['receive_step'] as String? ?? 'join';

    return Padding(
      padding: const EdgeInsets.all(24),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Text(receiveCopy?['title'] ?? 'Receive files',
              style: Theme.of(context).textTheme.headlineSmall),
          const SizedBox(height: 8),
          Text(receiveCopy?['description'] ?? ''),
          const SizedBox(height: 24),
          if (step == 'join' || step == 'connecting') ...[
            TextField(
              controller: _codeController,
              decoration: const InputDecoration(
                labelText: 'Join code',
                hintText: '64-character hex code',
              ),
              maxLines: 2,
            ),
            const SizedBox(height: 12),
            FilledButton(
              onPressed: _busy ? null : _join,
              child: const Text('Connect'),
            ),
          ],
          if (incoming > 0) ...[
            const SizedBox(height: 24),
            Text('$incoming file(s) ready'),
            const SizedBox(height: 12),
            FilledButton(
              onPressed: _busy ? null : _downloadAll,
              child: const Text('Download all'),
            ),
          ],
          if (ui?['error_message'] != null) ...[
            const SizedBox(height: 16),
            Text(ui!['error_message'] as String,
                style: TextStyle(color: Theme.of(context).colorScheme.error)),
          ],
        ],
      ),
    );
  }
}
