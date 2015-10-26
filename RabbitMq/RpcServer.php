<?php

namespace OldSound\RabbitMqBundle\RabbitMq;

use OldSound\RabbitMqBundle\RabbitMq\Exception\RequeueAndStopConsumerException;
use OldSound\RabbitMqBundle\RabbitMq\Exception\SendReplyAndStopConsumerException;
use PhpAmqpLib\Message\AMQPMessage;

class RpcServer extends BaseConsumer
{
    private $serializer = 'serialize';

    public function initServer($name, $type)
    {
        $this->disableAutoSetupFabric();
        $this->setExchangeOptions(array('name' => $name, 'type' => 'fanout'));
        $this->setQueueOptions(array('name' => $name . '-queue'));
    }

    public function processMessage(AMQPMessage $msg)
    {
        $sendReply = true;
        try {
            try {
                $result = call_user_func($this->callback, $msg);

                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
            }
            catch(SendReplyAndStopConsumerException $e) {
                $result = $e->getMessage();
                $this->forceStopConsumer();
            }
            catch(RequeueAndStopConsumerException $e) {
                $result = $e->getMessage();
                $this->forceStopConsumer();
                $sendReply = false;

                $msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], true);
            }
            $result = call_user_func($this->serializer, $result);
            if($sendReply) {
                $this->sendReply($result, $msg->get('reply_to'), $msg->get('correlation_id'));
            }
            $this->consumed++;
            $this->maybeStopConsumer();
        }
        catch (\Exception $e) {
            $this->sendReply('error: ' . $e->getMessage(), $msg->get('reply_to'), $msg->get('correlation_id'));
        }
    }

    protected function sendReply($result, $client, $correlationId)
    {
        $reply = new AMQPMessage($result, array('content_type' => 'text/plain', 'correlation_id' => $correlationId));
        $this->getChannel()->basic_publish($reply, '', $client);
    }

    public function setSerializer($serializer)
    {
        $this->serializer = $serializer;
    }
}
