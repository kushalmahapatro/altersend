import 'dart:convert';

import 'bridge/api.dart';

export 'bridge/api.dart'
    show buildJoinUrl, canJoinFromDeepLink, routeDownload;

/// Onboarding slide data from Rust domain.
Future<List<OnboardingSlide>> loadOnboardingSlides() async {
  try {
    final json = await getOnboardingSlidesJson();
    final list = jsonDecode(json) as List<dynamic>;
    return list
        .map((e) => OnboardingSlide.fromJson(e as Map<String, dynamic>))
        .toList();
  } catch (_) {
    return OnboardingSlide.defaults;
  }
}

class OnboardingSlide {
  const OnboardingSlide({
    required this.kind,
    required this.title,
    required this.subtitle,
    this.linkLabel,
    this.linkUrl,
  });

  final String kind;
  final String title;
  final String subtitle;
  final String? linkLabel;
  final String? linkUrl;

  factory OnboardingSlide.fromJson(Map<String, dynamic> json) {
    final link = json['link'] as Map<String, dynamic>?;
    return OnboardingSlide(
      kind: json['kind'] as String? ?? 'pairing',
      title: json['title'] as String? ?? '',
      subtitle: json['subtitle'] as String? ?? '',
      linkLabel: link?['label'] as String?,
      linkUrl: link?['url'] as String?,
    );
  }

  static const defaults = [
    OnboardingSlide(
      kind: 'pairing',
      title: 'Files, directly between devices.',
      subtitle:
          'One device sends, the other receives. A short code connects them so files can stream directly.',
    ),
    OnboardingSlide(
      kind: 'keep-open',
      title: 'Keep both apps open.',
      subtitle:
          'Files stream directly between devices — there is no cloud. Closing or backgrounding the app will cancel the transfer.',
    ),
    OnboardingSlide(
      kind: 'privacy',
      title: 'End-to-end encrypted.',
      subtitle:
          'No servers, no copies, no middlemen. Your files travel peer-to-peer between you and the recipient.',
      linkLabel: 'Read our privacy policy',
      linkUrl: 'https://altersend.com/privacy',
    ),
  ];
}
