use velr::{params, CellRef, QueryOptions, TableResult, Velr};

fn integers(table: &mut TableResult<'_>) -> velr::Result<Vec<i64>> {
    table.collect(|row| match row[0] {
        CellRef::Integer(value) => Ok(value),
        _ => panic!("expected integer row"),
    })
}

#[test]
fn table_can_be_read_after_its_execution_stream_is_dropped() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("UNWIND range(1, 10) AS i CREATE (:N {i: i})")?;

    let mut table = {
        let mut stream = db.exec("MATCH (n:N) RETURN n.i AS i ORDER BY i")?;
        stream.next_table()?.expect("one result table")
    };

    assert_eq!(integers(&mut table)?, (1..=10).collect::<Vec<_>>());
    drop(table);
    drop(db);
    Ok(())
}

#[test]
fn connection_can_move_to_another_thread_after_its_tables_are_dropped() -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("UNWIND range(1, 10) AS i CREATE (:N {i: i})")?;

    let mut table = db.exec_one("MATCH (n:N) RETURN n.i AS i ORDER BY i")?;
    assert_eq!(integers(&mut table)?, (1..=10).collect::<Vec<_>>());
    drop(table);

    let db = std::thread::spawn(move || -> velr::Result<Velr> {
        db.run("CREATE (:N {i: 11})")?;
        Ok(db)
    })
    .join()
    .expect("connection worker joins")?;

    let mut table = db.exec_one("MATCH (n:N) RETURN count(n) AS count")?;
    assert_eq!(integers(&mut table)?, vec![11]);
    drop(table);
    drop(db);
    Ok(())
}

#[derive(Clone, Copy)]
enum FinishTransaction {
    Commit,
    Rollback,
    Drop,
}

fn close_single_table_before_its_transaction(finish: FinishTransaction) -> velr::Result<()> {
    let db = Velr::open(None)?;
    db.run("UNWIND range(1, 10) AS i CREATE (:N {i: i})")?;

    for producer in 0..3 {
        for consumed in 0..3 {
            let tx = db.begin_tx()?;
            let mut table = match producer {
                0 => tx.exec_one("MATCH (n:N) RETURN n.i AS i")?,
                1 => {
                    tx.exec_one_with_options("MATCH (n:N) RETURN n.i AS i", QueryOptions::new())?
                }
                _ => tx.exec_one_with_params(
                    "MATCH (n:N) WHERE n.i >= $min RETURN n.i AS i",
                    params! { min: 1_i64 }?,
                )?,
            };

            match consumed {
                0 => {}
                1 => {
                    let mut rows = table.rows()?;
                    assert!(rows.next(|_| Ok(()))?);
                }
                _ => assert_eq!(integers(&mut table)?.len(), 10),
            }

            drop(table);
            match finish {
                FinishTransaction::Commit => tx.commit()?,
                FinishTransaction::Rollback => tx.rollback()?,
                FinishTransaction::Drop => drop(tx),
            }

            let mut count = db.exec_one("MATCH (n:N) RETURN count(n) AS count")?;
            assert_eq!(integers(&mut count)?, vec![10]);
        }
    }
    Ok(())
}

#[test]
fn single_tables_are_dropped_before_their_transaction_commits() -> velr::Result<()> {
    close_single_table_before_its_transaction(FinishTransaction::Commit)
}

#[test]
fn single_tables_are_dropped_before_their_transaction_rolls_back() -> velr::Result<()> {
    close_single_table_before_its_transaction(FinishTransaction::Rollback)
}

#[test]
fn single_tables_are_dropped_before_their_transaction_is_dropped() -> velr::Result<()> {
    close_single_table_before_its_transaction(FinishTransaction::Drop)
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn partial_bound_arrow_cursor_is_closed_before_transaction_finishes() -> velr::Result<()> {
    use arrow2::array::{Array, PrimitiveArray};

    let db = Velr::open(None)?;
    for finish in [
        FinishTransaction::Commit,
        FinishTransaction::Rollback,
        FinishTransaction::Drop,
    ] {
        let tx = db.begin_tx()?;
        let arrays: Vec<Box<dyn Array>> =
            vec![Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 3]))];
        tx.bind_arrow("_lifetime_rows", vec!["value".to_string()], arrays)?;

        {
            let mut table = tx.exec_one("UNWIND BIND('_lifetime_rows') AS row RETURN row.value")?;
            let mut rows = table.rows()?;
            assert!(rows.next(|row| {
                assert!(matches!(row[0], CellRef::Integer(1)));
                Ok(())
            })?);
        }

        match finish {
            FinishTransaction::Commit => tx.commit()?,
            FinishTransaction::Rollback => tx.rollback()?,
            FinishTransaction::Drop => drop(tx),
        }
        let mut table = db.exec_one("RETURN 42 AS value")?;
        assert_eq!(integers(&mut table)?, vec![42]);
    }
    Ok(())
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn connection_query_retains_transaction_bound_arrow_data_or_rejects_nested_execution(
) -> velr::Result<()> {
    use arrow2::array::PrimitiveArray;
    use std::sync::Arc;

    let db = Velr::open(None)?;
    db.run("CREATE (:LifetimeSeed)")?;
    let tx = db.begin_tx()?;
    let array = PrimitiveArray::<i64>::from_vec(vec![7, 11, 13]);
    let (allocation_owner, _, _) = array.values().clone().into_inner();
    let allocation = Arc::downgrade(&allocation_owner);
    drop(allocation_owner);
    tx.bind_arrow("_owned_rows", vec!["value".into()], vec![Box::new(array)])?;

    match db.exec_one("UNWIND BIND('_owned_rows') AS row RETURN row.value") {
        Ok(mut table) => {
            let mut rows = table.rows()?;
            let mut values = Vec::new();
            assert!(rows.next(|row| {
                let CellRef::Integer(value) = row[0] else {
                    panic!("expected Arrow integer");
                };
                values.push(value);
                Ok(())
            })?);
            drop(tx);
            assert!(
                allocation.upgrade().is_some(),
                "transaction cleanup freed data still used by a connection-query cursor"
            );
            while rows.next(|row| {
                let CellRef::Integer(value) = row[0] else {
                    panic!("expected Arrow integer");
                };
                values.push(value);
                Ok(())
            })? {}
            assert_eq!(values, [7, 11, 13]);
        }
        Err(error) => {
            assert!(
                error
                    .message
                    .contains("cannot start a transaction within a transaction"),
                "unexpected nested connection query error: {}",
                error.message
            );
            let mut table = tx.exec_one("UNWIND BIND('_owned_rows') AS row RETURN row.value")?;
            assert_eq!(integers(&mut table)?, [7, 11, 13]);
            drop(table);
            tx.rollback()?;
        }
    }
    drop(db);
    assert!(
        allocation.upgrade().is_none(),
        "Arrow allocation leaked after closing its owners"
    );
    Ok(())
}
