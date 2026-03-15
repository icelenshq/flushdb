#[derive(Debug)]
pub struct ReadBudget {
    remaining: u32,
    initial: u32,
    exhausted: bool,
}

impl ReadBudget {
    pub fn new(budget: u32) -> Self {
        Self {
            remaining: budget,
            initial: budget,
            exhausted: false,
        }
    }

    pub fn try_spend(&mut self) -> bool {
        if self.remaining > 0 {
            self.remaining -= 1;
            true
        } else {
            self.exhausted = true;
            false
        }
    }

    pub fn spend(&mut self, count: u32) {
        if count >= self.remaining {
            self.remaining = 0;
            self.exhausted = true;
        } else {
            self.remaining -= count;
        }
    }

    pub fn remaining(&self) -> u32 {
        self.remaining
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn used(&self) -> u32 {
        self.initial - self.remaining
    }
}
