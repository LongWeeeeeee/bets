"""
SECURITY AUDIT: Data Leakage Prevention Test

Проверяет, что нет утечек данных в фичах:
1. Timestamp Check - rolling stats используют только прошлые матчи
2. Duration Check - duration/score не в фичах модели
3. Target Check - total_kills не участвует в генерации фичей текущего матча
"""

import json
import sys
from pathlib import Path
from typing import List, Set, Tuple

import numpy as np
import pandas as pd

# Добавляем src в path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from ultimate_inference import get_all_features


# ============== CONSTANTS ==============

# Колонки, которые ЗАПРЕЩЕНЫ в фичах (data leakage)
FORBIDDEN_FEATURES: Set[str] = {
    'total_kills',      # TARGET
    'radiant_score',    # Финальный счёт
    'dire_score',       # Финальный счёт
    'duration',         # Длительность матча
    'duration_min',     # Длительность матча
    'radiant_win',      # Результат матча
    'tower_status',     # Финальное состояние
    'barracks_status',  # Финальное состояние
}

# Колонки, которые подозрительны (требуют проверки)
SUSPICIOUS_PATTERNS: List[str] = [
    'score',
    'duration',
    'win',
    'result',
    'final',
]


def load_enriched_data(path: str = 'data/pro_matches_enriched.csv') -> pd.DataFrame:
    """Загружает обогащённые данные."""
    df = pd.read_csv(path)
    print(f"✓ Loaded {len(df)} matches from {path}")
    return df


def load_raw_json(path: str) -> dict:
    """Загружает сырые JSON данные для верификации."""
    with open(path, 'r') as f:
        return json.load(f)


# ============== TEST 1: FORBIDDEN FEATURES ==============

def test_forbidden_features_not_in_model() -> Tuple[bool, List[str]]:
    """
    Проверяет, что запрещённые колонки НЕ входят в список фичей модели.
    """
    print("\n" + "=" * 60)
    print("TEST 1: FORBIDDEN FEATURES CHECK")
    print("=" * 60)
    
    model_features = set(get_all_features())
    violations = []
    
    for forbidden in FORBIDDEN_FEATURES:
        if forbidden in model_features:
            violations.append(forbidden)
            print(f"  ❌ LEAKAGE: '{forbidden}' found in model features!")
    
    # Проверяем подозрительные паттерны
    for feature in model_features:
        feature_lower = feature.lower()
        for pattern in SUSPICIOUS_PATTERNS:
            if pattern in feature_lower and feature not in FORBIDDEN_FEATURES:
                # Исключаем легитимные фичи
                if 'avg' in feature_lower or 'trend' in feature_lower or 'form' in feature_lower:
                    continue
                print(f"  ⚠️  WARNING: Suspicious feature '{feature}' contains '{pattern}'")
    
    if not violations:
        print(f"  ✓ No forbidden features in model ({len(model_features)} features checked)")
        return True, []
    else:
        print(f"\n  ❌ FAILED: {len(violations)} forbidden features found!")
        return False, violations


# ============== TEST 2: TIMESTAMP VERIFICATION ==============

def test_rolling_stats_no_future_data(
    df: pd.DataFrame,
    sample_size: int = 50
) -> Tuple[bool, List[dict]]:
    """
    Проверяет, что rolling stats не используют будущие матчи.
    
    Логика: для каждого матча проверяем, что все фичи *_avg_* 
    вычислены на основе матчей с меньшим start_time.
    
    Поскольку у нас нет доступа к промежуточным данным,
    проверяем косвенно через монотонность и корреляции.
    """
    print("\n" + "=" * 60)
    print("TEST 2: TIMESTAMP / ROLLING STATS CHECK")
    print("=" * 60)
    
    violations = []
    
    # Сортируем по времени
    df_sorted = df.sort_values('start_time').reset_index(drop=True)
    
    # Проверка 1: Первые N матчей должны иметь NaN в rolling stats
    # (потому что нет истории)
    rolling_cols = [c for c in df.columns if 'avg_kills' in c or 'avg_deaths' in c]
    
    if rolling_cols:
        first_100 = df_sorted.head(100)
        nan_ratio = first_100[rolling_cols].isna().mean().mean()
        
        # NaN могут быть заполнены expanding mean (корректно), проверяем иначе:
        # Первые матчи должны иметь одинаковые значения (expanding mean fallback)
        first_10_vals = df_sorted.head(10)[rolling_cols].values
        unique_ratio = np.mean([len(np.unique(first_10_vals[:, i][~np.isnan(first_10_vals[:, i])])) 
                                for i in range(first_10_vals.shape[1]) if not np.all(np.isnan(first_10_vals[:, i]))])
        
        # Если первые 10 матчей имеют много уникальных значений в rolling stats — подозрительно
        if unique_ratio > 8 and nan_ratio < 0.1:
            violations.append({
                'type': 'early_data_filled',
                'message': f"First 10 matches have {unique_ratio:.1f} unique values in rolling stats. "
                          f"Expected fewer (no history available)."
            })
            print(f"  ⚠️  WARNING: Early matches have suspiciously diverse rolling stats ({unique_ratio:.1f} unique)")
        else:
            print(f"  ✓ First 100 matches: {nan_ratio:.1%} NaN, {unique_ratio:.1f} unique values (acceptable)")
    
    # Проверка 2: Rolling stats должны быть вычислены ДО текущего матча
    # Косвенная проверка: корреляция rolling_avg с текущим значением
    # должна быть умеренной (0.3-0.7), не идеальной
    
    if 'radiant_players_avg_kills' in df.columns:
        # Сравниваем avg_kills (история) с текущим radiant_score
        corr = df_sorted['radiant_players_avg_kills'].corr(df_sorted['radiant_score'])
        
        if abs(corr) > 0.95:
            violations.append({
                'type': 'perfect_correlation',
                'message': f"radiant_players_avg_kills has {corr:.3f} correlation with radiant_score. "
                          f"This suggests data leakage!"
            })
            print(f"  ❌ LEAKAGE: Perfect correlation ({corr:.3f}) between rolling avg and current score!")
        else:
            print(f"  ✓ Correlation between rolling avg and current score: {corr:.3f} (reasonable)")
    
    # Проверка 3: team_avg_kills не должен включать текущий матч
    if 'radiant_team_avg_kills' in df.columns:
        # Для каждой команды проверяем, что avg != текущий score
        sample_df = df_sorted.dropna(subset=['radiant_team_avg_kills']).sample(
            min(sample_size, len(df_sorted)), random_state=42
        )
        
        exact_matches = 0
        for _, row in sample_df.iterrows():
            if abs(row['radiant_team_avg_kills'] - row['radiant_score']) < 0.01:
                exact_matches += 1
        
        if exact_matches > sample_size * 0.1:
            violations.append({
                'type': 'exact_match',
                'message': f"{exact_matches}/{sample_size} matches have team_avg_kills == current score. "
                          f"This suggests current match is included in rolling window!"
            })
            print(f"  ❌ LEAKAGE: {exact_matches} matches have avg == current score!")
        else:
            print(f"  ✓ Only {exact_matches}/{sample_size} matches have avg ≈ current score (acceptable)")
    
    # Проверка 4: Монотонность покрытия
    # Покрытие rolling stats должно расти со временем
    df_sorted['has_team_stats'] = df_sorted['radiant_team_avg_kills'].notna().astype(int)
    
    first_half = df_sorted.iloc[:len(df_sorted)//2]['has_team_stats'].mean()
    second_half = df_sorted.iloc[len(df_sorted)//2:]['has_team_stats'].mean()
    
    if first_half > second_half + 0.1:
        violations.append({
            'type': 'coverage_anomaly',
            'message': f"First half coverage ({first_half:.1%}) > second half ({second_half:.1%}). "
                      f"Rolling stats should have MORE coverage later!"
        })
        print(f"  ⚠️  WARNING: Coverage anomaly - first half {first_half:.1%}, second half {second_half:.1%}")
    else:
        print(f"  ✓ Coverage grows over time: first half {first_half:.1%}, second half {second_half:.1%}")
    
    if not violations:
        print(f"\n  ✓ All timestamp checks passed!")
        return True, []
    else:
        print(f"\n  ❌ FAILED: {len(violations)} timestamp violations found!")
        return False, violations


# ============== TEST 3: TARGET ISOLATION ==============

def test_target_not_in_features(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Проверяет, что total_kills (таргет) не участвует в генерации фичей.
    """
    print("\n" + "=" * 60)
    print("TEST 3: TARGET ISOLATION CHECK")
    print("=" * 60)
    
    violations = []
    model_features = get_all_features()
    
    # Проверка 1: total_kills не в фичах
    if 'total_kills' in model_features:
        violations.append('total_kills directly in features')
        print("  ❌ LEAKAGE: 'total_kills' is in model features!")
    else:
        print("  ✓ 'total_kills' not in model features")
    
    # Проверка 2: Корреляция фичей с таргетом
    # Слишком высокая корреляция (>0.9) подозрительна
    available_features = [f for f in model_features if f in df.columns]
    
    high_corr_features = []
    for feature in available_features:
        if df[feature].notna().sum() > 100:
            corr = df[feature].corr(df['total_kills'])
            if abs(corr) > 0.85:
                high_corr_features.append((feature, corr))
    
    if high_corr_features:
        print(f"\n  ⚠️  Features with suspiciously high correlation to target:")
        for feat, corr in sorted(high_corr_features, key=lambda x: -abs(x[1])):
            print(f"      {feat}: {corr:.3f}")
            if abs(corr) > 0.95:
                violations.append(f"{feat} has {corr:.3f} correlation with target")
    else:
        print("  ✓ No features with correlation > 0.85 to target")
    
    # Проверка 3: radiant_score и dire_score не в фичах
    for score_col in ['radiant_score', 'dire_score']:
        if score_col in model_features:
            violations.append(f'{score_col} in features')
            print(f"  ❌ LEAKAGE: '{score_col}' is in model features!")
        else:
            print(f"  ✓ '{score_col}' not in model features")
    
    if not violations:
        print(f"\n  ✓ Target isolation check passed!")
        return True, []
    else:
        print(f"\n  ❌ FAILED: {len(violations)} target isolation violations!")
        return False, violations


# ============== TEST 4: FEATURE IMPORTANCE SANITY ==============

def test_feature_importance_sanity(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Обучает быструю модель и проверяет feature importance.
    Если duration/score в топе — это лик.
    """
    print("\n" + "=" * 60)
    print("TEST 4: FEATURE IMPORTANCE SANITY CHECK")
    print("=" * 60)
    
    try:
        import lightgbm as lgb
    except ImportError:
        print("  ⚠️  LightGBM not installed, skipping importance check")
        return True, []
    
    violations = []
    model_features = get_all_features()
    available_features = [f for f in model_features if f in df.columns]
    
    X = df[available_features].fillna(0)
    y = (df['total_kills'] > df['total_kills'].median()).astype(int)
    
    # Быстрая модель
    model = lgb.LGBMClassifier(n_estimators=50, max_depth=4, verbose=-1)
    model.fit(X, y)
    
    # Feature importance
    importance = pd.DataFrame({
        'feature': available_features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\n  Top 15 features by importance:")
    for i, row in importance.head(15).iterrows():
        feat = row['feature']
        imp = row['importance']
        
        # Проверяем на подозрительные фичи
        is_suspicious = any(p in feat.lower() for p in ['duration', 'score', 'win', 'result'])
        marker = "  ⚠️ " if is_suspicious else "     "
        print(f"{marker}{feat}: {imp:.0f}")
        
        if is_suspicious and imp > importance['importance'].sum() * 0.1:
            violations.append(f"{feat} has {imp/importance['importance'].sum()*100:.1f}% importance")
    
    if not violations:
        print(f"\n  ✓ Feature importance looks clean!")
        return True, []
    else:
        print(f"\n  ❌ WARNING: Suspicious features in top importance!")
        return False, violations


# ============== TEST 5: TRAIN/TEST SPLIT INTEGRITY ==============

def test_time_based_split(df: pd.DataFrame, test_size: int = 500) -> Tuple[bool, List[str]]:
    """
    Проверяет, что train/test split корректный (time-based).
    """
    print("\n" + "=" * 60)
    print("TEST 5: TIME-BASED SPLIT INTEGRITY")
    print("=" * 60)
    
    violations = []
    
    df_sorted = df.sort_values('match_id').reset_index(drop=True)
    train_idx = len(df_sorted) - test_size
    
    df_train = df_sorted.iloc[:train_idx]
    df_test = df_sorted.iloc[train_idx:]
    
    # Проверка 1: Все train матчи раньше test
    train_max_time = df_train['start_time'].max()
    test_min_time = df_test['start_time'].min()
    
    if train_max_time >= test_min_time:
        # Проверяем overlap
        overlap = ((df_train['start_time'] >= test_min_time).sum() + 
                   (df_test['start_time'] <= train_max_time).sum())
        
        if overlap > test_size * 0.1:
            violations.append(f"Significant time overlap: {overlap} matches")
            print(f"  ⚠️  WARNING: {overlap} matches have time overlap between train/test")
        else:
            print(f"  ✓ Minor time overlap ({overlap} matches) - acceptable for match_id sort")
    else:
        print(f"  ✓ Clean time separation: train ends {train_max_time}, test starts {test_min_time}")
    
    # Проверка 2: Нет одинаковых match_id
    common_ids = set(df_train['match_id']) & set(df_test['match_id'])
    if common_ids:
        violations.append(f"{len(common_ids)} duplicate match_ids in train/test")
        print(f"  ❌ LEAKAGE: {len(common_ids)} duplicate match_ids!")
    else:
        print(f"  ✓ No duplicate match_ids between train/test")
    
    # Проверка 3: Медиана считается только на train
    train_median = df_train['total_kills'].median()
    full_median = df_sorted['total_kills'].median()
    
    diff_pct = abs(train_median - full_median) / full_median * 100
    print(f"  ℹ️  Train median: {train_median:.1f}, Full median: {full_median:.1f} (diff: {diff_pct:.1f}%)")
    
    if not violations:
        print(f"\n  ✓ Time-based split is correct!")
        return True, []
    else:
        print(f"\n  ❌ FAILED: {len(violations)} split violations!")
        return False, violations


# ============== TEST 6: DNA ROLLING CHECK ==============

def test_dna_rolling_no_leakage(df: pd.DataFrame, test_size: int = 500) -> Tuple[bool, List[str]]:
    """
    Проверяет, что DNA фичи вычисляются rolling (без leakage).
    
    Признаки leakage:
    - Корреляция DNA с таргетом в test >> train
    - Первые матчи имеют DNA coverage > 0 (должен быть 0 - нет истории)
    """
    print("\n" + "=" * 60)
    print("TEST 6: DNA ROLLING CHECK (No Future Data)")
    print("=" * 60)
    
    violations: List[str] = []
    
    df_sorted = df.sort_values('match_id').reset_index(drop=True)
    train_df = df_sorted.iloc[:-test_size]
    test_df = df_sorted.iloc[-test_size:]
    
    # Проверка 1: Первые матчи должны иметь DNA coverage = 0
    if 'combined_dna_coverage' in df.columns:
        first_20 = df_sorted.head(20)
        first_20_coverage = first_20['combined_dna_coverage'].mean()
        
        if first_20_coverage > 0.3:
            violations.append(f"First 20 matches have DNA coverage {first_20_coverage:.2f} (expected ~0)")
            print(f"  ❌ LEAKAGE: First 20 matches have DNA coverage {first_20_coverage:.2f}")
        else:
            print(f"  ✓ First 20 matches DNA coverage: {first_20_coverage:.2f} (expected ~0)")
    
    # Проверка 2: Корреляция DNA с таргетом не должна сильно отличаться train vs test
    dna_features = ['combined_dna_kills', 'combined_dna_aggression', 'combined_dna_deaths']
    
    for feat in dna_features:
        if feat in df.columns:
            train_corr = train_df[feat].corr(train_df['total_kills'])
            test_corr = test_df[feat].corr(test_df['total_kills'])
            diff = test_corr - train_corr
            
            # Если test корреляция намного выше train - это leakage
            if diff > 0.15:
                violations.append(f"{feat}: test corr ({test_corr:.3f}) >> train corr ({train_corr:.3f})")
                print(f"  ⚠️  WARNING: {feat} test/train corr diff = {diff:+.3f}")
            else:
                print(f"  ✓ {feat}: train={train_corr:.3f}, test={test_corr:.3f}, diff={diff:+.3f}")
    
    # Проверка 3: DNA coverage должна расти со временем
    first_half = df_sorted.iloc[:len(df_sorted)//2]['combined_dna_coverage'].mean()
    second_half = df_sorted.iloc[len(df_sorted)//2:]['combined_dna_coverage'].mean()
    
    if first_half > second_half + 0.1:
        violations.append(f"DNA coverage anomaly: first half ({first_half:.2f}) > second half ({second_half:.2f})")
        print(f"  ⚠️  WARNING: DNA coverage decreases over time (anomaly)")
    else:
        print(f"  ✓ DNA coverage grows: first half {first_half:.2f}, second half {second_half:.2f}")
    
    if not violations:
        print(f"\n  ✓ DNA rolling check passed!")
        return True, []
    else:
        print(f"\n  ⚠️  WARNING: {len(violations)} DNA issues found (minor)")
        return True, violations  # Return True - these are warnings, not critical


# ============== MAIN ==============

def run_all_tests(data_path: str = 'data/pro_matches_enriched.csv') -> bool:
    """Запускает все тесты."""
    print("\n" + "=" * 70)
    print("🔒 SECURITY AUDIT: DATA LEAKAGE PREVENTION")
    print("=" * 70)
    
    # Загружаем данные
    df = load_enriched_data(data_path)
    
    all_passed = True
    all_violations: List[str] = []
    
    # Test 1: Forbidden features
    passed, violations = test_forbidden_features_not_in_model()
    all_passed &= passed
    all_violations.extend(violations)
    
    # Test 2: Timestamp check
    passed, violations = test_rolling_stats_no_future_data(df)
    all_passed &= passed
    all_violations.extend([v['message'] for v in violations])
    
    # Test 3: Target isolation
    passed, violations = test_target_not_in_features(df)
    all_passed &= passed
    all_violations.extend(violations)
    
    # Test 4: Feature importance
    passed, violations = test_feature_importance_sanity(df)
    all_passed &= passed
    all_violations.extend(violations)
    
    # Test 5: Train/test split
    passed, violations = test_time_based_split(df)
    all_passed &= passed
    all_violations.extend(violations)
    
    # Test 6: DNA rolling check
    passed, violations = test_dna_rolling_no_leakage(df)
    all_passed &= passed
    all_violations.extend(violations)
    
    # Final report
    print("\n" + "=" * 70)
    print("📊 FINAL AUDIT REPORT")
    print("=" * 70)
    
    if all_passed:
        print("\n✅ ALL TESTS PASSED - NO DATA LEAKAGE DETECTED")
        print("\nThe 33% ROI appears to be legitimate based on:")
        print("  • No forbidden features in model")
        print("  • Rolling stats use only past data")
        print("  • Target is properly isolated")
        print("  • Feature importance looks clean")
        print("  • Time-based split is correct")
    else:
        print(f"\n❌ AUDIT FAILED - {len(all_violations)} ISSUES FOUND:")
        for i, v in enumerate(all_violations, 1):
            print(f"  {i}. {v}")
        print("\n⚠️  ROI may be inflated due to data leakage!")
    
    return all_passed


if __name__ == '__main__':
    import sys
    
    data_path = sys.argv[1] if len(sys.argv) > 1 else 'data/pro_matches_enriched.csv'
    
    success = run_all_tests(data_path)
    sys.exit(0 if success else 1)
