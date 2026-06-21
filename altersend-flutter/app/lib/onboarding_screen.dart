import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

import 'bridge_extensions.dart';
import 'onboarding_storage.dart';

class OnboardingScreen extends StatefulWidget {
  const OnboardingScreen({super.key, required this.onFinish});

  final VoidCallback onFinish;

  @override
  State<OnboardingScreen> createState() => _OnboardingScreenState();
}

class _OnboardingScreenState extends State<OnboardingScreen> {
  final _controller = PageController();
  List<OnboardingSlide> _slides = OnboardingSlide.defaults;
  int _index = 0;

  @override
  void initState() {
    super.initState();
    loadOnboardingSlides().then((slides) {
      if (mounted && slides.isNotEmpty) {
        setState(() => _slides = slides);
      }
    });
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  Future<void> _finish() async {
    await markOnboardingCompleted();
    widget.onFinish();
  }

  void _next() {
    if (_index >= _slides.length - 1) {
      _finish();
      return;
    }
    _controller.nextPage(
      duration: const Duration(milliseconds: 300),
      curve: Curves.easeInOut,
    );
  }

  IconData _iconFor(String kind) {
    switch (kind) {
      case 'keep-open':
        return Icons.phonelink;
      case 'privacy':
        return Icons.lock_outline;
      default:
        return Icons.devices;
    }
  }

  @override
  Widget build(BuildContext context) {
    final isLast = _index >= _slides.length - 1;

    return Scaffold(
      body: SafeArea(
        child: Column(
          children: [
            Expanded(
              child: PageView.builder(
                controller: _controller,
                itemCount: _slides.length,
                onPageChanged: (i) => setState(() => _index = i),
                itemBuilder: (context, i) {
                  final s = _slides[i];
                  return Padding(
                    padding: const EdgeInsets.symmetric(horizontal: 32),
                    child: Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        Icon(
                          _iconFor(s.kind),
                          size: 96,
                          color: Theme.of(context).colorScheme.primary,
                        ),
                        const SizedBox(height: 32),
                        Text(
                          s.title,
                          textAlign: TextAlign.center,
                          style: Theme.of(context).textTheme.headlineSmall,
                        ),
                        const SizedBox(height: 16),
                        Text(
                          s.subtitle,
                          textAlign: TextAlign.center,
                          style: Theme.of(context).textTheme.bodyLarge,
                        ),
                        if (s.linkUrl != null && s.linkLabel != null) ...[
                          const SizedBox(height: 16),
                          TextButton(
                            onPressed: () {
                              launchUrl(
                                Uri.parse(s.linkUrl!),
                                mode: LaunchMode.externalApplication,
                              );
                            },
                            child: Text(s.linkLabel!),
                          ),
                        ],
                      ],
                    ),
                  );
                },
              ),
            ),
            Row(
              mainAxisAlignment: MainAxisAlignment.center,
              children: List.generate(
                _slides.length,
                (i) => Container(
                  width: 8,
                  height: 8,
                  margin: const EdgeInsets.symmetric(horizontal: 4),
                  decoration: BoxDecoration(
                    shape: BoxShape.circle,
                    color: i == _index
                        ? Theme.of(context).colorScheme.primary
                        : Theme.of(context).colorScheme.outlineVariant,
                  ),
                ),
              ),
            ),
            const SizedBox(height: 24),
            Padding(
              padding: const EdgeInsets.fromLTRB(24, 0, 24, 24),
              child: SizedBox(
                width: double.infinity,
                child: FilledButton(
                  onPressed: _next,
                  child: Text(isLast ? 'Get started' : 'Continue'),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
