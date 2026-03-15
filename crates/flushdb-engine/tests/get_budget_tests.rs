use flushdb_engine::ReadBudget;

#[test]
fn test_budget_spend() {
    let mut budget = ReadBudget::new(3);

    assert!(budget.try_spend());
    assert!(budget.try_spend());
    assert!(budget.try_spend());
    assert!(!budget.try_spend());
}

#[test]
fn test_budget_exhausted_flag() {
    let mut budget = ReadBudget::new(1);

    assert!(!budget.is_exhausted());
    assert!(budget.try_spend());
    assert!(!budget.is_exhausted());

    assert!(!budget.try_spend());
    assert!(budget.is_exhausted());
}

#[test]
fn test_budget_used_tracking() {
    let mut budget = ReadBudget::new(8);
    budget.spend(3);

    assert_eq!(budget.used(), 3);
    assert_eq!(budget.remaining(), 5);
}

#[test]
fn test_budget_spend_multiple() {
    let mut budget = ReadBudget::new(5);
    budget.spend(3);

    assert_eq!(budget.remaining(), 2);
    assert!(!budget.is_exhausted());
}

#[test]
fn test_budget_spend_saturating() {
    let mut budget = ReadBudget::new(3);
    budget.spend(10);

    assert_eq!(budget.remaining(), 0);
    assert!(budget.is_exhausted());
}

#[test]
fn test_budget_fresh() {
    let budget = ReadBudget::new(5);

    assert_eq!(budget.remaining(), 5);
    assert_eq!(budget.used(), 0);
    assert!(!budget.is_exhausted());
}
