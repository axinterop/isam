import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomPool {
    private final List<Integer> pool = new ArrayList<>();
    private Random rnd = new Random();

    public RandomPool(int fromInclusive, int toExclusive) {
        for (int i = fromInclusive; i < toExclusive; i++) {
            pool.add(i);
        }
    }

    public RandomPool(Random rnd, int fromInclusive, int toExclusive) {
        this.rnd = rnd;
        for (int i = fromInclusive; i < toExclusive; i++) {
            pool.add(i);
        }
    }

    public boolean isEmpty() {
        return pool.isEmpty();
    }

    public int next() {
        if (pool.isEmpty()) {
            throw new IllegalStateException("Pool empty");
        }
        int idx = rnd.nextInt(pool.size());
        return pool.remove(idx);
    }
}
