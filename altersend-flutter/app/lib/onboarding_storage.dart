import 'package:shared_preferences/shared_preferences.dart';

const _completedKey = 'altersend.onboarding.completed';

Future<bool> isOnboardingCompleted() async {
  final prefs = await SharedPreferences.getInstance();
  return prefs.getBool(_completedKey) ?? false;
}

Future<void> markOnboardingCompleted() async {
  final prefs = await SharedPreferences.getInstance();
  await prefs.setBool(_completedKey, true);
}
