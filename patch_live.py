import re
import os

filepath = "d:\\crypto-momentum-intelligence\\research\\live_top_coins.py"
with open(filepath, "r", encoding="utf-8") as f:
    text = f.read()

# Replace linear_bias blend with direct meta_prob
old_bias = """        meta = MetaLR(max_iter=1000, solver="lbfgs", random_state=42)
        linear_bias = float(os.getenv("STACKING_LINEAR_BIAS", "0.35"))
        linear_bias = min(max(linear_bias, 0.0), 0.8)
        if len(meta_x) < 20 or len(np.unique(meta_y)) < 2:"""
new_bias = """        meta = MetaLR(max_iter=1000, solver="lbfgs", random_state=42)
        if len(meta_x) < 20 or len(np.unique(meta_y)) < 2:"""
text = text.replace(old_bias, new_bias)

old_meta_prob = """            ])
            meta_prob = meta.predict_proba(base_score)[:, 1]
            # Blend meta output with base logistic output to bias toward linear behavior.
            prob = (1.0 - linear_bias) * meta_prob + linear_bias * base_score[:, 0]

            # ── Feature importances: meta-weight × base-learner importance ──
            meta_w = np.abs(meta.coef_[0])  # [lr, xgb, rf, et]
            meta_w = meta_w / (meta_w.sum() or 1.0)
            print(
                f"[STACKING] Meta-weights: LR={meta_w[0]:.3f} XGB={meta_w[1]:.3f} "
                f"RF={meta_w[2]:.3f} ET={meta_w[3]:.3f} | linear_bias={linear_bias:.2f}"
            )"""
new_meta_prob = """            ])
            prob = meta.predict_proba(base_score)[:, 1]

            # ── Feature importances: meta-weight × base-learner importance ──
            meta_w = np.abs(meta.coef_[0])  # [lr, xgb, rf, et]
            meta_w = meta_w / (meta_w.sum() or 1.0)
            print(
                f"[STACKING] Meta-weights: LR={meta_w[0]:.3f} XGB={meta_w[1]:.3f} "
                f"RF={meta_w[2]:.3f} ET={meta_w[3]:.3f}"
            )"""
text = text.replace(old_meta_prob, new_meta_prob)

# Remove trigger mask
start_trigger = "# ── Momentum trigger gate before model prediction ──"
end_trigger = "print(\"[TRIGGER] No rows passed momentum trigger — skipping scoring cycle\")\n            return"

pattern_trigger = re.compile(re.escape(start_trigger) + r".*?" + re.escape(end_trigger), re.DOTALL)
text = pattern_trigger.sub("# ── Momentum trigger gate REMOVED (scoring all valid tokens natively) ──", text)

# Replace the training loop blocks
start_training = """        for ch, idx in chain_map.items():
            if len(idx) < 50:
                continue"""

end_training = """        for i, m in enumerate(meta):
            ch = (m.get("chain") or "base").lower()

            if ch not in chain_models:
                ch = next(iter(chain_models))

            probs[i] = chain_models[ch][i]"""

new_training = """        chain_thresholds = {}
        import numpy as np
        
        for ch, idx in chain_map.items():
            n_rows = len(idx)
            if n_rows < 100:
                print(f"[{ch}] Only {n_rows} rows — skipping, no model trained")
                continue

            cx = x_train[idx]
            cy = y_train[idx]
            sw = sample_weights[idx] if sample_weights is not None else None

            curr_model = args.model
            if n_rows < 400 and curr_model == "stacking":
                curr_model = "logistic"
                print(f"[{ch}] Only {n_rows} rows — using fallback simple setup")

            # Calibrate threshold on validation split
            split_idx = int(n_rows * 0.8)
            cx_train, cx_val = cx[:split_idx], cx[split_idx:]
            cy_train, cy_val = cy[:split_idx], cy[split_idx:]
            sw_train = sw[:split_idx] if sw is not None else None

            p_val, _, _ = score_live(
                model_type=curr_model,
                x_train=cx_train,
                y_train=cy_train,
                x_score=cx_val,
                robust=(args.preprocessing == "robust"),
                feature_names=feature_names,
                sample_weights=sw_train,
            )

            try:
                from sklearn.metrics import precision_recall_curve
                precision, recall, ths = precision_recall_curve(cy_val, p_val)
                viable = np.where(precision[:-1] >= 0.65)[0]
                if len(viable) > 0:
                    optimal_threshold = ths[viable[0]]
                else:
                    opt_idx = np.argmax(2 * precision[:-1] * recall[:-1] / (precision[:-1] + recall[:-1] + 1e-9))
                    optimal_threshold = ths[opt_idx]
            except Exception as e:
                print(f"[{ch}] Threshold calibration error: {e}. Defaulting to 0.55")
                optimal_threshold = 0.55
            
            chain_thresholds[ch] = float(optimal_threshold)
            print(f"[{ch}] Calibrated threshold: {float(optimal_threshold):.4f}")

            # Train on full block
            p, tuned_params, imp = score_live(
                model_type=curr_model,
                x_train=cx,
                y_train=cy,
                x_score=x_score,
                robust=(args.preprocessing == "robust"),
                feature_names=feature_names,
                sample_weights=sw,
                model_save_path=args.model_path,
            )

            chain_models[ch] = p
            tuned = tuned_params
            importances = imp

        # choose score based on token chain

        for i, m in enumerate(meta):
            ch = (m.get("chain") or "base").lower()

            # Ensure ch is string and correctly default if necessary
            if not chain_models:
                probs[i] = 0.0
                continue
                
            if ch not in chain_models:
                ch = next(iter(chain_models))

            # Apply a heavy penalty if under threshold to prevent it from out-ranking true BUY signals
            p_val = chain_models[ch][i]
            if p_val < chain_thresholds.get(ch, 0.55):
                p_val = p_val * 0.1
            probs[i] = p_val"""

pattern_training = re.compile(re.escape(start_training) + r".*?" + re.escape(end_training), re.DOTALL)
text = pattern_training.sub(new_training, text)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(text)
print("Patched live_top_coins.py perfectly!")
