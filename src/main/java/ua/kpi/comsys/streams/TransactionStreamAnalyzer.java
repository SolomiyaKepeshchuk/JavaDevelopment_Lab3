package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

public class TransactionStreamAnalyzer {

    public Optional<Transaction> findFirstHighValueTransaction(
            Stream<Transaction> stream, BigDecimal threshold) {

        if (stream == null || threshold == null) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        return stream.sequential()
                .dropWhile(t -> t.amount().compareTo(threshold) <= 0)
                .findFirst();
    }

    public List<Transaction> takeTransactionsUntilBudgetExceeded(
            Stream<Transaction> stream, BigDecimal maxBudget) {

        if (stream == null || maxBudget == null) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        BigDecimal[] total = {BigDecimal.ZERO};

        return stream.sequential()
                .takeWhile(t -> {
                    total[0] = total[0].add(t.amount());
                    return total[0].compareTo(maxBudget) <= 0;
                })
                .toList();
    }

    public List<List<Transaction>> processBatches(
            Stream<Transaction> stream, int batchSize, int maxBatches) {

        if (stream == null || batchSize <= 0 || maxBatches <= 0) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        Gatherer<Transaction, List<Transaction>, List<Transaction>> gatherer =
                Gatherer.ofSequential(
                        ArrayList::new,
                        (batch, t, downstream) -> {
                            batch.add(t);

                            if (batch.size() == batchSize) {
                                boolean result = downstream.push(List.copyOf(batch));
                                batch.clear();
                                return result;
                            }

                            return true;
                        }
                );

        return stream.sequential()
                .gather(gatherer)
                .limit(maxBatches)
                .toList();
    }
}