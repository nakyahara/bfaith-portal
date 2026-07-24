# -*- coding: utf-8 -*-
"""otsu_binarize (旧cv2.threshold置き換え) の検証

OpenCV削除時 (2026-07-23 省メモリ化) に、cv2.threshold(THRESH_BINARY+THRESH_OTSU) と
705ケースでピクセル一致することを検証済み。本テストはOpenCVに依存せず、
素朴な全探索実装をオラクルとして同一性を守る。
"""
import unittest

import numpy as np

try:
    from barcode_extractor import otsu_binarize
    HAVE_DEPS = True
except ImportError as e:
    # skipするのは pyzbar (zbar DLL) 未導入の場合のみ。それ以外のimport失敗は
    # 本体の回帰なので隠さず落とす (Codexレビュー PR#606 low)
    if 'pyzbar' not in str(e):
        raise
    HAVE_DEPS = False


def naive_otsu_threshold(arr):
    """教科書どおりの全探索・大津の閾値 (検算用。速度は気にしない)"""
    hist = [0] * 256
    for v in arr.ravel():
        hist[int(v)] += 1
    total = arr.size
    best_t, best_var = 0, -1.0
    for t in range(256):
        w_bg = sum(hist[:t + 1])
        w_fg = total - w_bg
        if w_bg == 0 or w_fg == 0:
            continue
        mean_bg = sum(i * hist[i] for i in range(t + 1)) / w_bg
        mean_fg = sum(i * hist[i] for i in range(t + 1, 256)) / w_fg
        var = w_bg * w_fg * (mean_bg - mean_fg) ** 2
        if var > best_var:
            best_var, best_t = var, t
    return best_t


@unittest.skipUnless(HAVE_DEPS, 'pyzbar未導入環境ではskip')
class OtsuBinarizeTest(unittest.TestCase):
    def assert_matches_naive(self, arr):
        expected = np.where(arr > naive_otsu_threshold(arr), 255, 0).astype(np.uint8)
        actual = otsu_binarize(arr)
        self.assertTrue(np.array_equal(expected, actual))
        self.assertEqual(actual.dtype, np.uint8)
        self.assertTrue(set(np.unique(actual)) <= {0, 255})

    def test_random_arrays_match_naive(self):
        rng = np.random.default_rng(20260723)
        for _ in range(30):
            h, w = rng.integers(5, 80, 2)
            self.assert_matches_naive(rng.integers(0, 256, (h, w), dtype=np.uint8))

    def test_barcode_like_bimodal(self):
        # 白地に黒バー (バーコード類似)。バーと地がきれいに分かれること
        rng = np.random.default_rng(7)
        arr = np.full((40, 120), 235, dtype=np.uint8)
        arr[:, 10:13] = 20
        arr[:, 30:32] = 25
        arr[:, 70:75] = 15
        noise = rng.normal(0, 5, arr.shape).astype(np.int32)
        arr = np.clip(arr.astype(np.int32) + noise, 0, 255).astype(np.uint8)
        self.assert_matches_naive(arr)
        binary = otsu_binarize(arr)
        self.assertEqual(binary[:, 11].max(), 0)    # バーは黒
        self.assertEqual(binary[:, 50].min(), 255)  # 地は白

    def test_edge_cases(self):
        self.assert_matches_naive(np.zeros((5, 5), dtype=np.uint8))
        self.assert_matches_naive(np.full((5, 5), 255, dtype=np.uint8))
        self.assert_matches_naive(np.full((5, 5), 128, dtype=np.uint8))
        self.assert_matches_naive(np.array([[0, 255]], dtype=np.uint8))
        self.assert_matches_naive(np.arange(256, dtype=np.uint8).reshape(16, 16))


if __name__ == '__main__':
    unittest.main()
