package org.week07


import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.Decimal

case class LixinliRue(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left, right, true) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.isInstanceOf[Decimal] &&
      right.asInstanceOf[Literal].value.asInstanceOf[Decimal].toDouble == 1.0 =>
      logInfo("自定义规则：LXL")
      left
  }
}


class LxlExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      new LixinliRue(session)
    }
  }
}
