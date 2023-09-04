pub trait DataVisitor {
    type Error;

    fn visit(
        &mut self,
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<bool, Self: Error>;

    fn post_visit(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}


pub struct CompressedChunkBuilderVisitor {
    pub chunk: CompressedChunk,
}

pub struct CollectorVisitor {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

impl CollectorVisitor {
    fn new(values_capacity: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(values_capacity),
            values: Vec::with_capacity(values_capacity),
        }
    }
}
impl DataVisitor for CollectorVisitor {
    type Error = Error;

    fn visit(
        &mut self,
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<bool, Self::Error> {
        self.timestamps.extend_from_slice(timestamps);
        self.values.extend_from_slice(values);
        Ok(true)
    }
}