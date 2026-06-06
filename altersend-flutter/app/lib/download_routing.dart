import 'dart:io' show Platform;

import 'package:flutter/foundation.dart';
import 'package:gal/gal.dart';

import 'bridge/api.dart';

const _imageExtensions = {
  'jpg',
  'jpeg',
  'png',
  'gif',
  'heic',
  'heif',
  'webp',
  'bmp',
  'tiff',
  'tif',
};

const _videoExtensions = {
  'mov',
  'mp4',
  'm4v',
  '3gp',
  'avi',
  'mkv',
  'webm',
};

bool get _isMobile =>
    !kIsWeb && (Platform.isAndroid || Platform.isIOS);

String _extension(String fileName) {
  final dot = fileName.lastIndexOf('.');
  if (dot == -1 || dot == fileName.length - 1) return '';
  return fileName.substring(dot + 1).toLowerCase();
}

bool isMediaFile(String fileName) {
  final ext = _extension(fileName);
  return _imageExtensions.contains(ext) || _videoExtensions.contains(ext);
}

class DownloadRoutingResult {
  const DownloadRoutingResult({
    required this.intended,
    required this.destination,
    required this.localPath,
  });

  final String intended;
  final String destination;
  final String localPath;
}

Future<DownloadRoutingResult> handleDownloadedFile(
  String localPath,
  String fileName,
) async {
  if (!_isMobile || !isMediaFile(fileName)) {
    return DownloadRoutingResult(
      intended: 'filesystem',
      destination: 'filesystem',
      localPath: localPath,
    );
  }

  try {
    final hasAccess = await Gal.hasAccess();
    if (!hasAccess) {
      final granted = await Gal.requestAccess();
      if (!granted) {
        return DownloadRoutingResult(
          intended: 'photos',
          destination: 'filesystem',
          localPath: localPath,
        );
      }
    }
    final ext = _extension(fileName);
    if (_videoExtensions.contains(ext)) {
      await Gal.putVideo(localPath, album: 'AlterSend');
    } else {
      await Gal.putImage(localPath, album: 'AlterSend');
    }
    return DownloadRoutingResult(
      intended: 'photos',
      destination: 'photos',
      localPath: localPath,
    );
  } catch (_) {
    return DownloadRoutingResult(
      intended: 'photos',
      destination: 'filesystem',
      localPath: localPath,
    );
  }
}

Future<void> notifyDownloadRouted({
  required String offerKey,
  required DownloadRoutingResult routing,
}) async {
  await routeDownload(
    offerKey: offerKey,
    savedTo: routing.localPath,
    destination: routing.destination,
    intendedDestination: routing.intended,
  );
}
