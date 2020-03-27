package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @scala.annotation.tailrec
    def recBalance(chars: List[Char], balance: Int): Boolean =
      chars match {
        case Nil => balance == 0
        case x :: xs => x match {
          case '(' => recBalance(xs, balance + 1)
          case ')' => if (balance == 0) {
            false
          } else {
            recBalance(xs, balance - 1)
          }
          case _ => recBalance(xs, balance)
        }
      }

    recBalance(chars.toList, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(from: Int, until: Int): (Int, Int) = {
      var unbalancedFromLeft = 0
      var unbalancedFromRight = 0
      var idx = from
      while (idx < until) {
        chars(idx) match {
          case ')' => if (unbalancedFromRight > 0) {
            unbalancedFromRight -= 1
          } else {
            unbalancedFromLeft += 1
          }
          case '(' =>
            unbalancedFromRight += 1
          case _ =>
        }

        idx += 1
      }
      (unbalancedFromLeft, unbalancedFromRight)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until)
      } else {
        val mid = from + (until - from) / 2
        val ((l1, r1), (l2, r2)) = parallel(reduce(from, mid), reduce(mid, until))

        val diff = r1 - l2
        if (diff > 0) {
          (l1, r2 + diff)
        } else if (diff < 0) {
          (l1 - diff, r2)
        } else {
          (l1, r2)
        }
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
