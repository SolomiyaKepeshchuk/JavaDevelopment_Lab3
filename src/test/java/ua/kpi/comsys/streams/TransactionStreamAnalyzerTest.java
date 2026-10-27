package ua.kpi.comsys.streams;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TransactionStreamAnalyzerTest {

    private final TransactionStreamAnalyzer analyzer =
            new TransactionStreamAnalyzer();

    private Transaction transaction(int id, int amount) {
        return new Transaction(
                "T" + id,
                "Shopping",
                BigDecimal.valueOf(amount),
                LocalDateTime.now()
        );
    }

    @Test
    void testFindFirstHighValueTransaction() {
        Transaction t1 = transaction(1, 50);
        Transaction t2 = transaction(2, 150);
        Transaction t3 = transaction(3, 200);

        Optional<Transaction> result =
                analyzer.findFirstHighValueTransaction(
                        Stream.of(t1, t2, t3),
                        BigDecimal.valueOf(100)
                );

        assertEquals(Optional.of(t2), result);
    }

    @Test
    void testNoHighValueTransaction() {
        Optional<Transaction> result =
                analyzer.findFirstHighValueTransaction(
                        Stream.of(transaction(1, 20), transaction(2, 50)),
                        BigDecimal.valueOf(100)
                );

        assertTrue(result.isEmpty());
    }

    @Test
    @Timeout(2)
    void testInfiniteTransactionStream() {
        Stream<Transaction> stream = Stream.iterate(1, n -> n + 1)
                .map(n -> transaction(n, n * 10));

        Optional<Transaction> result =
                analyzer.findFirstHighValueTransaction(
                        stream, BigDecimal.valueOf(100)
                );

        assertEquals("T11",
                result.map(Transaction::transactionId).orElse(""));
    }

    @Test
    void testTransactionsWithinBudget() {
        Transaction t1 = transaction(1, 20);
        Transaction t2 = transaction(2, 30);
        Transaction t3 = transaction(3, 60);

        List<Transaction> result =
                analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.of(t1, t2, t3),
                        BigDecimal.valueOf(50)
                );

        assertEquals(List.of(t1, t2), result);
    }

    @Test
    void testFirstTransactionExceedsBudget() {
        List<Transaction> result =
                analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.of(transaction(1, 100), transaction(2, 20)),
                        BigDecimal.valueOf(50)
                );

        assertTrue(result.isEmpty());
    }

    @Test
    @Timeout(2)
    void testInfiniteBudgetStream() {
        Stream<Transaction> stream =
                Stream.generate(() -> transaction(1, 10));

        List<Transaction> result =
                analyzer.takeTransactionsUntilBudgetExceeded(
                        stream, BigDecimal.valueOf(35)
                );

        assertEquals(3, result.size());
    }

    @Test
    void testProcessBatches() {
        Transaction t1 = transaction(1, 10);
        Transaction t2 = transaction(2, 20);
        Transaction t3 = transaction(3, 30);
        Transaction t4 = transaction(4, 40);

        List<List<Transaction>> result =
                analyzer.processBatches(
                        Stream.of(t1, t2, t3, t4), 2, 2
                );

        assertEquals(
                List.of(List.of(t1, t2), List.of(t3, t4)),
                result
        );
    }

    @Test
    @Timeout(2)
    void testInfiniteBatches() {
        Stream<Transaction> stream = Stream.iterate(1, n -> n + 1)
                .map(n -> transaction(n, 10));

        List<List<Transaction>> result =
                analyzer.processBatches(stream, 3, 2);

        assertEquals(2, result.size());
        assertEquals(3, result.get(0).size());
        assertEquals(3, result.get(1).size());
        assertEquals("T6", result.get(1).get(2).transactionId());
    }

    @Test
    void testNullArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstHighValueTransaction(
                        null, BigDecimal.TEN));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstHighValueTransaction(
                        Stream.empty(), null));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.takeTransactionsUntilBudgetExceeded(
                        null, BigDecimal.TEN));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.empty(), null));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(null, 2, 2));
    }

    @Test
    void testInvalidBatchParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 0, 2));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 2, 0));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), -1, 2));

        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 2, -1));
    }
}